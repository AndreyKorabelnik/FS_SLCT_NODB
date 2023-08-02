import json
import pandas as pd
import general
import os
import sql_expr_parser


def if_add_attr_to_current_level(attr, known_attrs, universe_data):
    if 'partition_by' in attr and attr['partition_by'] not in known_attrs:
        # if there are dependencies on new attributes don't add it to current level
        return False
    if attr['attr_type'] == 'INPUT':
        # inputs always go
        pass
    elif attr['attr_type'] == 'RANK':
        for rank_attr in attr['rank_attrs']:
            # if there are dependencies on new attributes don't add it to current level
            if rank_attr['attr_code'] not in known_attrs:
                return False
    elif attr['attr_type'] == 'AGGREGATE':
        # if there are dependencies on new attributes don't add it to current level
        if attr['aggregate_attr_code'] not in known_attrs:
            return False
    elif attr['attr_type'] == 'EXPRESSION':
        parsed_expression = sql_expr_parser.parse(attr['expression'])
        for identifier in sql_expr_parser.extract_identifiers(parsed_expression):
            # find the identifier in universe_data
            expr_attr = next(attr for attr in universe_data['attributes'] if attr['attr_code'] == identifier)
            if expr_attr['attr_code'] not in known_attrs:
                return False
    return True


def get_leveled_attributes(universe_data):
    leveled_attrs = dict()
    level_num = -1
    while len(universe_data['attributes']) > len(leveled_attrs):
        level_num += 1
        known_attrs = [attr for attr in leveled_attrs if leveled_attrs[attr] < level_num]
        for attr in (attr for attr in universe_data['attributes'] if attr['attr_code'] not in known_attrs):
            if if_add_attr_to_current_level(attr, known_attrs, universe_data):
                leveled_attrs[attr['attr_code']] = level_num
    return leveled_attrs


def get_df_column_name(attr_code, universe_data, level):
    attr_type = next(attr['attr_type'] for attr in universe_data['attributes'] if attr['attr_code'] == attr_code)
    if attr_type == 'INPUT':
        return attr_code
    else:
        return f"{attr_code}_{level}"


def transform_expression_to_level(filter_expression, universe_data, level):
    idents = sql_expr_parser.extract_identifiers(sql_expr_parser.parse(filter_expression))
    for ident in sorted(idents, key=lambda x: len(str(x)), reverse=True):
        filter_expression = str(filter_expression).upper().replace(ident, get_df_column_name(ident, universe_data, level))
    return filter_expression


# add attribute columns to df starting from lowest level
# todo: make sure that all INPUT attributes are in input_data_file
# todo: add condition that all preceding levels filters are True for RANK and AGGREGATE
def add_level_attributes(df, universe_data, level_attributes, level, selection_id):
    preceding_filters_column = f"is_selected_{selection_id}_level_{level-1}"

    leveled_attrs = get_leveled_attributes(universe_data)
    # run through all attributes in universe_data sorted by level from leveled_attrs
    for attr in sorted(level_attributes, key=lambda x: leveled_attrs[x['attr_code']]):
        df_column_name = get_df_column_name(attr['attr_code'], universe_data, level)
        if attr['attr_type'] == 'RANK':
            rank_attrs = [get_df_column_name(a['attr_code'], universe_data, level)
                          for a in sorted(attr['rank_attrs'], key=lambda x: x['order'])]
            if level > 1:
                # apply preceding filters
                rank_attrs.insert(0,preceding_filters_column)
            #  todo: note that it takes only first rank attribute's direction,
            #   need to support separate direction for each attr
            is_ascending = True if attr['rank_attrs'][0]['direction'] == 'ASC' else False
            if 'partition_by' in attr and attr['partition_by']:
                ranks = df.groupby(get_df_column_name(attr['partition_by'], universe_data, level))[rank_attrs].apply(
                    tuple).rank(method='first',
                                ascending=is_ascending)
            else:
                ranks = df[rank_attrs].apply(tuple).rank(method='first',
                                                         ascending=is_ascending)
            # without reseting all values are NaN in df
            ranks.reset_index(drop=True, inplace=True)
            df[df_column_name] = ranks
        elif attr['attr_type'] == 'AGGREGATE':
            aggr_attr = get_df_column_name(attr['aggregate_attr_code'], universe_data, level)
            if level > 1:
                # apply preceding filters
                df[f'{aggr_attr}_aux'] = df.apply(lambda row: row[aggr_attr] if row[preceding_filters_column] else 0,
                                                  axis=1)
                aggr_attr = f'{aggr_attr}_aux'
            aggr_func = attr['aggregate_function']
            # todo: aggr_func not sum
            if 'partition_by' in attr and attr['partition_by']:
                aggrs = df.groupby(get_df_column_name(attr['partition_by'], universe_data, level))[aggr_attr].sum()
            else:
                aggrs = df[aggr_attr].sum()
            df[df_column_name] = aggrs
        elif attr['attr_type'] == 'EXPRESSION':
            expression = transform_expression_to_level(attr['expression'], universe_data, level)
            df[df_column_name] = eval(sql_expr_parser.transform_to_pandas(expression))
    return df

# add filter column after underlying attributes are added
# level needs to add own versions of attributes as there might be scenario like this
# level 1: filter_1: rank_by_turnover<100
# level 2: filter_2: rank_by_mcap<100
# level 3: filter_3: rank_by_turnover<10
# so filter_3 can't use rank_by_turnover calculated by filter_1 as there is rank_by_mcap<100 filter in between
def add_filter(df, selection_id, filter_id, filter_expression, universe_data, level):
    filter_expression = transform_expression_to_level(filter_expression, universe_data, level)
    df[f'filter_{selection_id}_{filter_id}'] = eval(sql_expr_parser.transform_to_pandas(filter_expression))
    return df


def get_failed_filters(row, selection_id):
    return ','.join(c for c in row.index[row == False].tolist() if c.startswith(f'filter_{selection_id}'))


def run_selections(selections, universe_data, input_file):
    df = pd.read_csv(input_file)
    for s in selections['selections']:
        for lvl in sorted(set(f['application_level'] for f in s['filters'])):
            # todo: add to level_attributes only attributes relevant to filters from the application level
            level_attributes = [attr for attr in universe_data['attributes']
                                if attr['attr_type'] != 'INPUT'
                                and attr['attr_code'] not in df.columns.tolist()]
            df = add_level_attributes(df, universe_data, level_attributes, lvl, s['selection_id'])
            for f in s['filters']:
                if f['application_level'] == lvl:
                    df = add_filter(df, s['selection_id'], f['filter_id'], f['expression'], universe_data, lvl)
            df[f"is_selected_{s['selection_id']}_level_{lvl}"] = df.eval(
                f" and ".join(f"filter_{s['selection_id']}_{f['filter_id']} == True"
                              for f in s['filters'] if f['application_level'] <= lvl))
        df[f"is_selected_{s['selection_id']}"] = df.eval(
            f" and ".join(f"filter_{s['selection_id']}_{f['filter_id']} == True" for f in s['filters']))
        df[f"failed_filters_{s['selection_id']}"] = df.apply(get_failed_filters, axis=1, selection_id=s['selection_id'])
    return df


def get_selection_results(selection_id, df):
    # show all attributes and only selection_id relevant "filter" and "is_selected" columns
    # todo: add options to
    #       - show only KEY attribute column,
    #       - don't show filter_, failed_filter_ columns
    relevant_columns = (c for c in df.columns.tolist()
                        if c.startswith(f"filter_{selection_id}_")
                        or c == f"is_selected_{selection_id}"
                        or c == f"failed_filters_{selection_id}"
                        or not any(c.startswith(substring) for substring in ['filter_',
                                                                             'is_selected_',
                                                                             'failed_filters_']))
    return df[relevant_columns]


def run(client_input_folder, client_output_folder):
    general.make_dir(client_output_folder)
    universe_file = os.path.join(client_input_folder, 'universe.json')
    selection_file = os.path.join(client_input_folder, 'selection.json')
    input_file = os.path.join(client_input_folder, 'input_data.csv')

    with open(universe_file, 'r') as file:
        universe_data = json.load(file)
    with open(selection_file, 'r') as file:
        selections = json.load(file)
    df_all_selections = run_selections(selections=selections,
                                       universe_data=universe_data,
                                       input_file=input_file)
    for s in selections['selections']:
        df_out = get_selection_results(s['selection_id'], df_all_selections)
        with open(f"{client_output_folder}/output_{s['selection_id']}.csv", 'w') as file:
            df_out.to_csv(file, index=False, lineterminator='')


if __name__ == '__main__':
    run('source_data', 'output/test')
