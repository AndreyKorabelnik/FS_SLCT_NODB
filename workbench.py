import json
import pandas as pd


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


def add_attributes(df):
    leveled_attributes = get_leveled_attributes(universe_data)
    max_level = max(attr[1] for attr in leveled_attributes)
    # level 0 input attributes are ignored, assumption is that they all are in input_data_file
    # todo: make sure that all level 0 attributes are in input_data_file
    for level in range(1, max_level + 1):
        level_attrs = [attr[0] for attr in leveled_attributes if attr[1] == level]
        for attr in (attr for attr in universe_data['attributes'] if attr['attr_code'] in level_attrs):
            if attr['attr_type'] == 'INPUT':
                pass  # inputs already there
            elif attr['attr_type'] == 'RANK':
                rank_attrs = [a['attr_code'] for a in sorted(attr['rank_attrs'], key=lambda a: a['order'])]
                #  todo: note that it takes only first rank attribute's direction,
                #   need to support separate direction for each attr
                is_ascending = True if attr['rank_attrs'][0]['direction'] == 'ASC' else False
                if 'partition_by' in attr and attr['partition_by']:
                    df[attr['attr_code']] = df.groupby(attr['partition_by'])[rank_attrs].apply(tuple).rank(
                        method='first',
                        ascending=is_ascending)
                else:
                    df[attr['attr_code']] = df[rank_attrs].apply(tuple).rank(method='first',
                                                                             ascending=is_ascending)
            # elif attr['attr_type'] == 'AGGREGATE':
            #     if attr['aggregate_attr_code'] not in known_attrs:
            #         add_to_current_level = 0
            # elif attr['attr_type'] == 'EXPRESSION':
            #     pass  # todo
    return df


universe_file = 'source_data/universe.json'
input_data_file = 'source_data/input_data.csv'
with open(universe_file, 'r') as file:
    universe_data = json.load(file)
df = pd.read_csv(input_data_file)
df = add_attributes(df)

print(len(df))
