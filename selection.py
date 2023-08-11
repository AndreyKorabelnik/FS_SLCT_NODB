import json
import os
import pandas as pd
import attributes
import general
import sql_expr_parser


def get_attribute(attr_code, universe_attributes):
    return next(attr for attr in universe_attributes if attr.code == attr_code)


def get_attribute_dependencies(attr_code, universe_attributes):
    dependencies = []
    parents = [attr_code]
    while parents:
        a = parents.pop()
        if a not in dependencies:
            dependencies.append(a)
            parents.extend(d for d in get_attribute(a, universe_attributes).get_dependencies()
                           if d not in parents)
    return reversed(dependencies)


def add_attribute(df, attr_code, universe_attributes, preceding_filters_column):
    for a in get_attribute_dependencies(attr_code, universe_attributes):
        # don't rewrite attr_code in df as it might have been added by a preceding filter
        if a not in df.columns.tolist():
            df = get_attribute(a, universe_attributes).add_to_dataframe(df, preceding_filters_column)
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
        df = run_selection(s['selection_id'],
                           s['filters'],
                           attributes.get_universe_attributes(universe['attributes']),
                           df)
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
