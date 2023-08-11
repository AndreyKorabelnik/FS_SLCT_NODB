import json
import os
import attributes
import general
import selections
import sql_expr_parser
import pandas as pd


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
    return dependencies


def add_attribute(df, attr_code, universe_attributes, preceding_filters_column):
    for a in reversed(get_attribute_dependencies(attr_code, universe_attributes)):
        # don't rewrite attr_code in df as it might have been added by a preceding filter
        if a not in df.columns.tolist():
            df = get_attribute(a, universe_attributes).add_to_dataframe(df, preceding_filters_column)
    return df


def get_failed_filters(row, selection_id):
    return '; '.join(c for c in row.index[row == False].tolist() if c.startswith(f'filter_{selection_id}'))


def run_selection(selection, universe_attributes, df):
    curr_filters_column = ''
    for lvl in selection.get_application_levels():
        for filter_id, expression, application_level in selection.get_filters():
            if application_level == lvl:
                filter_attributes = sql_expr_parser.extract_identifiers(sql_expr_parser.parse(expression))
                for attr_code in filter_attributes:
                    df = add_attribute(df, attr_code, universe_attributes, curr_filters_column)
                df[selection.get_filter_name(filter_id)] = eval(sql_expr_parser.transform_to_pandas(expression))
        curr_filters_column = f"is_selected_{selection.id}_level_{lvl}"
        df[curr_filters_column] = df.eval(
            f" and ".join(f"{selection.get_filter_name(filter_id)} == True"
                          for filter_id, _, application_level in selection.get_filters()
                          if application_level <= lvl))
    df[f"is_selected_{selection.id}"] = df.eval(
        f" and ".join(f"{selection.get_filter_name(filter_id)} == True"
                      for filter_id, _, _ in selection.get_filters()))
    df[f"failed_filters_{selection.id}"] = df.apply(get_failed_filters, axis=1, selection_id=selection.id)
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
        universe_src = json.load(file)
    with open(os.path.join(client_input_folder, 'selection.json'), 'r') as file:
        selections_src = json.load(file)
    df = pd.read_csv(os.path.join(client_input_folder, 'input_data.csv'))
    universe_attributes = attributes.get_universe_attributes(universe_src['attributes'])
    for selection in selections.get_selections(selections_src['selections']):
        df = run_selection(selection, universe_attributes, df)
        df_out = get_selection_results(selection.id, df)
        with open(f"{client_output_folder}/output_{selection.id}.csv", 'w') as file:
            df_out.to_csv(file, index=False, lineterminator='')


if __name__ == '__main__':
    run('source_data', 'output/test')
