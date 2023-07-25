import json
import pandas as pd
import sql_expr_parser


def get_leveled_attributes(universe_data):
    leveled_attrs = dict()
    level_num = -1
    while len(universe_data['attributes']) > len(leveled_attrs):
        level_num += 1
        known_attrs = [attr for attr in leveled_attrs if leveled_attrs[attr] < level_num]
        for attr in (attr for attr in universe_data['attributes'] if attr['attr_code'] not in known_attrs):
            add_to_current_level = 1
            if 'partition_by' in attr and attr['partition_by'] not in known_attrs:
                # if there are dependencies on new attributes don't add it to current level
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
                # if there are dependencies on new attributes don't add it to current level
                if attr['aggregate_attr_code'] not in known_attrs:
                    add_to_current_level = 0
            elif attr['attr_type'] == 'EXPRESSION':
                pass  # todo
            if add_to_current_level:
                leveled_attrs[attr['attr_code']] = level_num
    return leveled_attrs


def add_filter(df, selection_id, filter_id, filter_expression, leveled_attrs):
    parsed_expression = sql_expr_parser.parse(filter_expression)
    attributes = sql_expr_parser.extract_identifiers(parsed_expression)
    # add attribute columns to df starting from lowest level
    # todo: make sure that all INPUT attributes are in input_data_file
    for attr in sorted(universe_data['attributes'], key=lambda x: leveled_attrs[x['attr_code']]):
        if attr['attr_type'] != 'INPUT' and \
                attr['attr_code'] in attributes and attr['attr_code'] not in df.columns.tolist():
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
        # elif attr['attr_type'] == 'AGGREGATE':
        #     if attr['aggregate_attr_code'] not in known_attrs:
        #         add_to_current_level = 0
        # elif attr['attr_type'] == 'EXPRESSION':
        #     pass  # todo
        # add filter column after attributes added
    df[f'filter_{selection_id}_{filter_id}'] = eval(sql_expr_parser.transform_to_pandas(filter_expression))
    return df


universe_file = 'source_data/universe.json'
input_data_file = 'source_data/input_data_small.csv'
selection_file = 'source_data/selection.json'
with open(universe_file, 'r') as file:
    universe_data = json.load(file)
with open(selection_file, 'r') as file:
    selections = json.load(file)

leveled_attrs = get_leveled_attributes(universe_data)
df = pd.read_csv(input_data_file)
for s in selections['selections']:
    for f in sorted(s['filters'], key=lambda x: x['application_level']):
        df = add_filter(df, s['selection_id'], f['filter_id'], f['expression'], leveled_attrs)

pd.options.display.max_columns = 500
print(df)
