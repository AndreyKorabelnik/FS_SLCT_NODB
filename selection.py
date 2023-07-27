import general, os
import json
import pandas as pd
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


# add attribute columns to df starting from lowest level
# todo: make sure that all INPUT attributes are in input_data_file
def add_universe_attributes(df, universe_data):
    leveled_attrs = get_leveled_attributes(universe_data)
    relevant_attrs = [attr for attr in universe_data['attributes']
                      if attr['attr_type'] != 'INPUT'
                      and attr['attr_code'] not in df.columns.tolist()]
    # run through all attributes in universe_data sorted by level from leveled_attrs
    for attr in sorted(relevant_attrs, key=lambda x: leveled_attrs[x['attr_code']]):
        if attr['attr_type'] == 'RANK':
            rank_attrs = [a['attr_code'] for a in sorted(attr['rank_attrs'], key=lambda x: x['order'])]
            #  todo: note that it takes only first rank attribute's direction,
            #   need to support separate direction for each attr
            is_ascending = True if attr['rank_attrs'][0]['direction'] == 'ASC' else False
            if 'partition_by' in attr and attr['partition_by']:
                ranks = df.groupby(attr['partition_by'])[rank_attrs].apply(tuple).rank(
                    method='first',
                    ascending=is_ascending)
            else:
                ranks = df[rank_attrs].apply(tuple).rank(method='first',
                                                         ascending=is_ascending)
            # without reseting all values are NaN in df
            ranks.reset_index(drop=True, inplace=True)
            df[attr['attr_code']] = ranks
        elif attr['attr_type'] == 'EXPRESSION':
            df[attr['attr_code']] = eval(sql_expr_parser.transform_to_pandas(attr['expression']))
        elif attr['attr_type'] == 'AGGREGATE':
            aggr_attr = attr['aggregate_attr_code']
            aggr_func = attr['aggregate_function']
            # todo: aggr_func not sum
            if 'partition_by' in attr and attr['partition_by']:
                aggrs = df.groupby(attr['partition_by'])[aggr_attr].sum()
            else:
                aggrs = df[aggr_attr].sum()
            df[attr['attr_code']] = aggrs
    return df


def add_filter(df, selection_id, filter_id, filter_expression):
    # add filter column after underlying attributes are added
    # todo: add condition that all preceding filters are True
    df[f'filter_{selection_id}_{filter_id}'] = eval(sql_expr_parser.transform_to_pandas(filter_expression))
    return df


def get_failed_filters(row, selection_id):
    return ','.join(c for c in row.index[row == False].tolist() if c.startswith(f'filter_{selection_id}'))


def run_selections(selections, universe_data, input_file):
    df = pd.read_csv(input_file)
    df = add_universe_attributes(df, universe_data)
    for s in selections['selections']:
        for f in sorted(s['filters'], key=lambda x: x['application_level']):
            df = add_filter(df, s['selection_id'], f['filter_id'], f['expression'])
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


if __name__== '__main__':
    run('source_data','output/test')




