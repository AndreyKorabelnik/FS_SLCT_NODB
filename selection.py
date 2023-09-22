import json
import os
from typing import List, Set
from collections import defaultdict

import pandas as pd
import pandasql

import attributes
import selections
import sql_expr_parser

UNIVERSE_FILE_NAME = 'universe_dax.json'
SELECTIONS_FILE_NAME = 'selection_dax.json'
INPUT_DATA_FILE_NAME = 'input_data_dax.csv'


class InputDataFileNotFound(Exception):
    pass


class SelectionsFileError(Exception):
    pass


class UniverseFileError(Exception):
    pass


def get_inputs(client_input_folder: str) -> (pd.DataFrame, List[attributes.Attribute], List[selections.Selection]):
    """
    Extracts inputs from client_input_folder
    """
    try:
        df = pd.read_csv(os.path.join(client_input_folder, INPUT_DATA_FILE_NAME))
    except FileNotFoundError as e:
        raise InputDataFileNotFound(f"Input data file not found: {e}")
    try:
        with open(os.path.join(client_input_folder, UNIVERSE_FILE_NAME), 'r') as file:
            universe_src = json.load(file)
        universe_attributes = attributes.get_universe_attributes(universe_src['attributes'])
        key_column = universe_src['key']
    except (FileNotFoundError, json.JSONDecodeError) as e:
        raise UniverseFileError(f"Error loading Universe file: {e}")
    try:
        with open(os.path.join(client_input_folder, SELECTIONS_FILE_NAME), 'r') as file:
            selections_src = json.load(file)
        sels = selections.get_selections(selections_src['selections'])
    except (FileNotFoundError, json.JSONDecodeError) as e:
        raise SelectionsFileError(f"Error loading Selections file: {e}")
    return df, universe_attributes, sels, key_column


def get_ordered_attrs(selection: selections.Selection,
                      universe_attributes: List[attributes.Attribute],
                      application_level: int,
                      input_attrs: Set):
    attr_code_dependencies = defaultdict(list)
    for filter_id, expression in selection.get_filters(application_level):
        # get all attributes from filters of the application_level
        for attr_code in sql_expr_parser.extract_identifiers(expression):
            attr_code_dependencies[attr_code] = [dep for dep in attributes.get_attribute_dependencies(attr_code,
                                                                                                      universe_attributes)
                                                 if dep not in input_attrs]
    ordered_attrs = defaultdict(set)
    sql_level = 0
    flag = True
    while flag:
        flag = False
        for deps in attr_code_dependencies.values():
            if deps:
                ordered_attrs[sql_level].add(deps.pop())
                flag = True
        sql_level += 1
    return ordered_attrs


def add_attrs_to_sql_query(sql_query: str,
                           attr_codes: List[str],
                           universe_attributes: List[attributes.Attribute],
                           preceding_filters: List[str]):
    if attr_codes:
        columns = ','.join(
            attributes.get_attribute(attr_code, universe_attributes).get_sql_expression(preceding_filters)
            for attr_code in attr_codes)
        return f"select d.*,{columns} from ({sql_query}) d"
    else:
        return sql_query


def add_attrs_to_selection_sql(sql_query: str, selection: selections.Selection, application_level: int,
                               universe_attributes: List[attributes.Attribute], input_attrs: Set[str]) -> str:
    preceding_filters = [f"filters_level_{level}" for level in selection.get_application_levels() if
                         level < application_level]
    # add filters relevant attributes
    ordered_attrs = get_ordered_attrs(selection, universe_attributes, application_level, input_attrs)
    for attr_codes in ordered_attrs.values():
        sql_query = add_attrs_to_sql_query(sql_query, list(attr_codes), universe_attributes, preceding_filters)
    # add output attributes
    sql_query = add_attrs_to_sql_query(sql_query, selection.get_output_attrs(application_level),
                                       universe_attributes, preceding_filters)
    return sql_query


def add_filters_to_selection_sql(sql_query: str, selection: selections.Selection, application_level: int):
    filters = ','.join(f"case when {expression} then 1 else 0 end as filter_{filter_id}"
                       for filter_id, expression in selection.get_filters(application_level))
    sql_query = f"select d.*,{filters} from ({sql_query}) d"
    # add combined filters column
    aux_string = " and ".join(f"filter_{filter_id}=1" for filter_id, _ in selection.get_filters(application_level))
    sql_query = f"select d.*,case when {aux_string} then 1 else 0 end as filters_level_{application_level} " \
                f"from ({sql_query}) d"
    return sql_query


def add_is_selected_to_selection_sql(sql_query: str, selection: selections.Selection):
    aux_string = " and ".join(f"filters_level_{lvl}=1" for lvl in selection.get_application_levels())
    sql_query = f"select d.*,case when {aux_string} then 1 else 0 end as is_selected from ({sql_query}) d"
    return sql_query


def build_selection_sql(selection: selections.Selection, universe_attributes: List[attributes.Attribute]) -> str:
    """
    builds sql query to express selection process in sql
    """
    input_attrs = {a.code for a in universe_attributes if type(a) == attributes.AttributeInput}
    sql_query = 'select * from df'
    for lvl in selection.get_application_levels():
        sql_query = add_attrs_to_selection_sql(sql_query, selection, lvl, universe_attributes, input_attrs)
        sql_query = add_filters_to_selection_sql(sql_query, selection, lvl)
    sql_query = add_is_selected_to_selection_sql(sql_query, selection)
    return sql_query


def get_selection_results(selection: selections.Selection, key_column: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns df with attributes, filters relevant to selection
    """
    show_all, add_attributes, add_filters, add_failed_filters = selection.get_output_settings()
    relevant_columns = [key_column]
    if add_attributes:
        relevant_columns.extend(c for c in df.columns.tolist()
                                if c not in (key_column, "is_selected") and not c.startswith("filter"))
    if add_filters:
        relevant_columns.extend(c for c in df.columns.tolist()
                                if c.startswith("filter_"))
        if add_failed_filters:
            # Vectorized operation
            filter_cols = df.columns[df.columns.str.startswith('filter_')]
            df["failed_filters"] = df[filter_cols].apply(lambda x: ';'.join(x.index[x == 0].tolist()), axis=1)
            relevant_columns.append("failed_filters")
    if show_all:
        relevant_columns.append("is_selected")
        return df[relevant_columns]
    else:
        return df[df['is_selected'] == 1][relevant_columns]


# todo: make sure that all INPUT attributes are in input_data_file
def run(client_input_folder: str, client_output_folder: str):
    df, universe_attributes, sels, key_column = get_inputs(client_input_folder)
    for selection in sels:
        selection_sql = build_selection_sql(selection, universe_attributes)
        df_out = get_selection_results(selection, key_column, pandasql.sqldf(selection_sql))
        output_file_name = os.path.join(client_output_folder, f'output_{selection.get_id()}.csv')
        with open(output_file_name, 'w') as file:
            df_out.to_csv(file, index=False, lineterminator='\n')


if __name__ == '__main__':
    run('source_data', 'output/test')
