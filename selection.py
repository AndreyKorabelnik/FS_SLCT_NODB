import json
import os
from typing import List

import pandas as pd

import attributes
import selections
import sql_expr_parser

UNIVERSE_FILE_NAME = 'universe.json'
SELECTIONS_FILE_NAME = 'selection.json'
INPUT_DATA_FILE_NAME = 'input_data.csv'


def get_attribute(attr_code: str, universe_attributes: List[attributes.Attribute]) -> attributes.Attribute:
    """
    Returns Attribute type by attr_code from universe_attributes
    """
    return next(attr for attr in universe_attributes if attr.code == attr_code)


def get_attribute_dependencies(attr_code: str, universe_attributes: List[attributes.Attribute]) -> List[str]:
    """
    Returns list of parent attr_codes for attr_code
    """
    dependencies = []
    parents = [attr_code]
    while parents:
        a = parents.pop()
        if a not in dependencies:
            dependencies.append(a)
            parents.extend(d for d in get_attribute(a, universe_attributes).get_dependencies()
                           if d not in parents)
    return dependencies


def add_attribute(df: pd.DataFrame, attr_code: str, universe_attributes: List[attributes.Attribute],
                  preceding_filters_column: str) -> pd.DataFrame:
    """
    Adds attr_code to pandas data frame df
    """
    for a in reversed(get_attribute_dependencies(attr_code, universe_attributes)):
        # don't rewrite attr_code in df as it might have been added by a preceding filter
        if a not in df.columns.tolist():
            df = get_attribute(a, universe_attributes).add_to_dataframe(df, preceding_filters_column)
    return df


def get_failed_filters_columns(row, selection_id: int):
    """
    Returns string with names of failed filters in row
    """
    return '; '.join(c for c in row.index[row == False].tolist() if c.startswith(f'filter_{selection_id}'))


def run_selection(selection: selections.Selection, universe_attributes: List[attributes.Attribute],
                  df: pd.DataFrame) -> pd.DataFrame:
    curr_filters_column = ''
    selection_id = selection.get_id()
    for lvl in selection.get_application_levels():
        for filter_id, expression, application_level in selection.get_filters():
            if application_level == lvl:
                for attr_code in sql_expr_parser.extract_identifiers(sql_expr_parser.parse(expression)):
                    df = add_attribute(df, attr_code, universe_attributes, curr_filters_column)
                df[selection.get_filter_name(filter_id)] = eval(sql_expr_parser.transform_to_pandas(expression))
        curr_filters_column = f"is_selected_{selection_id}_level_{lvl}"
        df[curr_filters_column] = df.eval(
            " and ".join(f"{selection.get_filter_name(filter_id)} == True"
                         for filter_id, _, application_level in selection.get_filters() if application_level <= lvl))
    df[f"is_selected_{selection_id}"] = df.eval(" and ".join(f"{selection.get_filter_name(filter_id)} == True"
                                                             for filter_id, _, _ in selection.get_filters()))
    df[f"failed_filters_{selection_id}"] = df.apply(get_failed_filters_columns,
                                                    axis=1,
                                                    selection_id=selection_id)
    return df


def get_selection_results(selection: selections.Selection, df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns df with all attributes and only selection_id relevant "filter" and "is_selected" columns
    """
    # todo: add options to
    #       - show only KEY attribute column,
    #       - don't show filter_, failed_filter_ columns
    selection_id = selection.get_id()
    relevant_columns = (c for c in df.columns.tolist()
                        if c.startswith(f"filter_{selection_id}_")
                        or c == f"is_selected_{selection_id}"
                        or c == f"failed_filters_{selection_id}"
                        or not any(c.startswith(substring) for substring in ['filter_',
                                                                             'is_selected_',
                                                                             'failed_filters_']))
    return df[relevant_columns]


def get_inputs(client_input_folder: str) -> (pd.DataFrame, List[attributes.Attribute], List[selections.Selection]):
    """
    Extracts inputs from client_input_folder
    """
    with open(os.path.join(client_input_folder, UNIVERSE_FILE_NAME), 'r') as file:
        universe_src = json.load(file)
    with open(os.path.join(client_input_folder, SELECTIONS_FILE_NAME), 'r') as file:
        selections_src = json.load(file)
    df = pd.read_csv(os.path.join(client_input_folder, INPUT_DATA_FILE_NAME))
    universe_attributes = attributes.get_universe_attributes(universe_src['attributes'])
    sels = selections.get_selections(selections_src['selections'])
    return df, universe_attributes, sels


# todo: make sure that all INPUT attributes are in input_data_file
def run(client_input_folder: str, client_output_folder: str):
    df, universe_attributes, sels = get_inputs(client_input_folder)
    for selection in sels:
        df = run_selection(selection, universe_attributes, df)
        df_out = get_selection_results(selection, df)
        output_file_name = os.path.join(client_output_folder, f'output_{selection.get_id()}.csv')
        with open(output_file_name, 'w') as file:
            df_out.to_csv(file, index=False, lineterminator='')


if __name__ == '__main__':
    run('source_data', 'output/test')
