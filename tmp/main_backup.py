import pandas as pd
import pandasql
import json


def get_selections(selection_file):
    with open(selection_file) as file:
        selections_filters = json.loads(file.read())['results'][0]['items']
    selections = {sf['selection_id'] for sf in selections_filters}
    return selections, selections_filters


def get_selection_sql(selection_filters):
    filters_expr = '\n'.join(f",case when {sf['filter_expression']} then 1 else 0 end as filter_{sf['filter_id']}"
                             for sf in selection_filters)
    return f'select d.*\n{filters_expr}\nfrom df d'


def get_failed_filters(row):
    return ','.join(row.index[row == 0].tolist())


def run_selection(df, selection_id, selection_filters, output_directory, ouput_only_selected):
    selection_sql = get_selection_sql(selection_filters)
    # selection_sql = 'select d.*, min_country_code||min_ICB5_INDUSTRY_CODE,1 as is_selected from ('\
    #                 'select d.*,min(country_code) over () as min_country_code, ' \
    #                 '       min(ICB5_INDUSTRY_CODE) over () as min_ICB5_INDUSTRY_CODE ' \
    #                 'from df d) d'
    df_out = pandasql.sqldf(selection_sql)
    if selection_filters:
        df_out["is_selected"] = df_out.eval(f' and '.join(f"filter_{sf['filter_id']}==1" for sf in selection_filters))
        if ouput_only_selected:
            df_out = df_out[df_out['is_selected'] == 1]
        else:
            df_out["failed_filters"] = df_out.apply(get_failed_filters, axis=1)
    with open(f'{output_directory}/output_{selection_id}.csv', 'w') as file:
        df_out.to_csv(file, index=False, lineterminator='')


def run_selections(universe_file, selection_file, input_file, output_directory, ouput_only_selected=False):
    selections, selections_filters = get_selections(selection_file)
    df = pd.read_csv(input_file)
    for selection_id in selections:
        selection_filters = [sf for sf in selections_filters if sf['selection_id'] == selection_id]
        run_selection(df, selection_id, selection_filters, output_directory, ouput_only_selected)


if __name__ == '__main__':
    run_selections(universe_file='../source_data/universe.json',
                   selection_file='../source_data/selection_big.json',
                   input_file='../source_data/input_data.csv',
                   output_directory='output',
                   ouput_only_selected=1)