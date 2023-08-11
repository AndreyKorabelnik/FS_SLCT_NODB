import json
import os

import pandas as pd

import general
import sql_expr_parser


def get_attribute_dependencies_rank(attr):
    r = [a['attr_code'] for a in attr['rank_attrs']]
    if 'partition_by' in attr and attr['partition_by']:
        r.append(attr['partition_by'])
    return r


def get_attribute_dependencies_aggregate(attr):
    r = [attr['aggregate_attr_code']]
    if 'partition_by' in attr and attr['partition_by']:
        r.append(attr['partition_by'])
    return r


def get_attribute_dependencies_expression(attr):
    return sql_expr_parser.extract_identifiers(sql_expr_parser.parse(attr['expression']))


def get_attribute_dependencies(attr_code, universe_attributes):
    dependencies = []
    parents = [attr_code]
    while parents:
        a = parents.pop()
        if a not in dependencies:
            attr = next(attr for attr in universe_attributes if attr['attr_code'] == a)
            if attr['attr_type'] != 'INPUT':
                dependencies.append(a)
                if attr['attr_type'] == 'RANK':
                    direct_dependencies = get_attribute_dependencies_rank(attr)
                elif attr['attr_type'] == 'AGGREGATE':
                    direct_dependencies = get_attribute_dependencies_aggregate(attr)
                elif attr['attr_type'] == 'EXPRESSION':
                    direct_dependencies = get_attribute_dependencies_expression(attr)
                parents.extend(d for d in direct_dependencies if d not in parents)
    return reversed(dependencies)


def add_attribute_rank(df, attr, preceding_filters_column):
    rank_attrs = [(a['attr_code'], a['direction'])
                  for a in sorted(attr['rank_attrs'], key=lambda x: x['order'])]
    if preceding_filters_column:
        # apply preceding filters first. it ranks DESC to make rows passed filters more priority
        rank_attrs.insert(0, (preceding_filters_column, 'DESC'))
    combine_rank_attr = f"{attr['attr_code']}_aux"
    df[combine_rank_attr] = df.apply(lambda row: tuple((-1 if direction == 'DESC' else 1) * row[a]
                                                       for (a, direction) in rank_attrs), axis=1)
    if 'partition_by' in attr and attr['partition_by']:
        ranks = df.groupby(attr['partition_by'])[combine_rank_attr].rank(method='first')
    else:
        ranks = df[combine_rank_attr].rank(method='first')
    # without reseting all values are NaN in df
    ranks.reset_index(drop=True, inplace=True)
    df[attr['attr_code']] = ranks
    return df


def add_attribute_aggregate(df, attr, preceding_filters_column):
    aggr_attr = attr['aggregate_attr_code']
    if preceding_filters_column:
        # apply preceding filters first
        df[f'{aggr_attr}_aux'] = df.apply(
            lambda row: row[aggr_attr] if row[preceding_filters_column] else 0,
            axis=1)
        aggr_attr = f'{aggr_attr}_aux'
    aggr_func = attr['aggregate_function']
    if 'partition_by' in attr and attr['partition_by']:
        aggrs = df.groupby(attr['partition_by'])[aggr_attr].apply(aggr_func)
    else:
        aggrs = df[aggr_attr].apply(aggr_func)
    df[attr['attr_code']] = aggrs
    return df


def add_attribute_expression(df, attr):
    df[attr['attr_code']] = eval(sql_expr_parser.transform_to_pandas(attr['expression']))
    return df


def add_attribute(df, attr_code, universe_attributes, preceding_filters_column):
    for a in get_attribute_dependencies(attr_code, universe_attributes):
        # don't rewrite attr_code in df as it might have been added by a preceding filter
        if a not in df.columns.tolist():
            attr = next(attr for attr in universe_attributes if attr['attr_code'] == a)
            if attr['attr_type'] == 'RANK':
                df = add_attribute_rank(df, attr, preceding_filters_column)
            elif attr['attr_type'] == 'AGGREGATE':
                df = add_attribute_aggregate(df, attr, preceding_filters_column)
            elif attr['attr_type'] == 'EXPRESSION':
                df = add_attribute_expression(df, attr)
    return df


def get_failed_filters(row, selection_id):
    return '; '.join(c for c in row.index[row == False].tolist() if c.startswith(f'filter_{selection_id}'))


def get_filter_name(selection_id, filter_id):
    return f"filter_{selection_id}_{filter_id}"


def run_selection(selection_id, filters, universe_attributes, df):
    curr_filters_column = ''
    for lvl in sorted(set(f['application_level'] for f in filters)):
        level_filters = [f for f in filters if f['application_level'] == lvl]
        for f in level_filters:
            # add filter attributes
            for attr_code in sql_expr_parser.extract_identifiers(sql_expr_parser.parse(f['expression'])):
                df = add_attribute(df, attr_code, universe_attributes, curr_filters_column)
            # add filter after its attributes are added
            df[get_filter_name(selection_id, f['filter_id'])] = eval(
                sql_expr_parser.transform_to_pandas(f['expression']))
        curr_filters_column = f"is_selected_{selection_id}_level_{lvl}"
        df[curr_filters_column] = df.eval(
            f" and ".join(f"{get_filter_name(selection_id, f['filter_id'])} == True"
                          for f in filters if f['application_level'] <= lvl))
    df[f"is_selected_{selection_id}"] = df.eval(
        f" and ".join(f"{get_filter_name(selection_id, f['filter_id'])} == True" for f in filters))
    df[f"failed_filters_{selection_id}"] = df.apply(get_failed_filters, axis=1, selection_id=selection_id)
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


# todo: make sure that all INPUT attributes are in input_data_file
def run(client_input_folder, client_output_folder):
    general.make_dir(client_output_folder)
    with open(os.path.join(client_input_folder, 'universe.json'), 'r') as file:
        universe = json.load(file)
    with open(os.path.join(client_input_folder, 'selection.json'), 'r') as file:
        selections = json.load(file)
    df = pd.read_csv(os.path.join(client_input_folder, 'input_data.csv'))
    for s in selections['selections']:
        df = run_selection(s['selection_id'], s['filters'], universe['attributes'], df)
        df_out = get_selection_results(s['selection_id'], df)
        with open(f"{client_output_folder}/output_{s['selection_id']}.csv", 'w') as file:
            df_out.to_csv(file, index=False, lineterminator='')


def test():
    with open(os.path.join('source_data', 'universe.json'), 'r') as file:
        universe = json.load(file)
    print(get_attribute_dependencies('LISTING_EXPRESSION', universe['attributes']))


if __name__ == '__main__':
    run('source_data', 'output/test')
    # test()
