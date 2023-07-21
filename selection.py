import pandas as pd
import pandasql
import json
import os
import general


def get_selections(selection_file):
    with open(selection_file) as file:
        selections_filters = json.loads(file.read())['results'][0]['items']
    selections = {sf['selection_id'] for sf in selections_filters}
    return selections, selections_filters


def get_leveled_attributes(universe_data):
    level_attrs = list()
    level_num = -1
    while len(universe_data['attributes']) > len(level_attrs):
        level_num += 1
        known_attrs = [a[0] for a in level_attrs if a[1] < level_num]
        for attr in (attr for attr in universe_data['attributes'] if attr['attr_code'] not in known_attrs):
            add_to_current_level = 1
            if 'partition_by' in attr and attr['partition_by'] not in known_attrs:
                add_to_current_level = 0
                break
            if attr['attr_type'] == 'INPUT':
                pass  # inputs always go
            elif attr['attr_type'] == 'RANK':
                for rank_attr in attr['rank_attrs']:
                    # if there are dependencies on new attributes don't add it to current level
                    if rank_attr['attr_code'] not in known_attrs:
                        add_to_current_level = 0
                        break
            elif attr['attr_type'] == 'AGGREGATE':
                if attr['aggregate_attr_code'] not in known_attrs:
                    add_to_current_level = 0
            elif attr['attr_type'] == 'EXPRESSION':
                pass  # todo
            if add_to_current_level:
                level_attrs.append((attr['attr_code'], level_num))
    return level_attrs


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


def run(client_input_folder, client_output_folder):
    general.make_dir(client_output_folder)
    run_selections(universe_file=os.path.join(client_input_folder, 'universe.json'),
                   selection_file=os.path.join(client_input_folder, 'selection.json'),
                   input_file=os.path.join(client_input_folder, 'input_data.csv'),
                   output_directory=client_output_folder,
                   ouput_only_selected=1)

