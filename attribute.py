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

def get_attribute_dependencies_expression(attr):
    return sql_expr_parser.extract_identifiers(sql_expr_parser.parse(attr['expression']))


class Attribute:
    def __init__(self, attr_data):
        self.attr_data = attr_data

    def get_dependencies(self, universe_attributes):
        raise NotImplementedError("Subclasses should implement this method.")

    def add_to_dataframe(self, df, preceding_filters_column):
        raise NotImplementedError("Subclasses should implement this method.")


class AttributeRank(Attribute):
    def get_dependencies(self, universe_attributes):
        return get_attribute_dependencies_rank(self.attr_data)

    def add_to_dataframe(self, df, preceding_filters_column):
        return add_attribute_rank(df, self.attr_data, preceding_filters_column)


class AttributeAggregate(Attribute):
    def get_dependencies(self, universe_attributes):
        return get_attribute_dependencies_aggregate(self.attr_data)

    def add_to_dataframe(self, df, preceding_filters_column):
        return add_attribute_aggregate(df, self.attr_data, preceding_filters_column)


class AttributeExpression(Attribute):
    def get_dependencies(self, universe_attributes):
        return get_attribute_dependencies_expression(self.attr_data)

    def add_to_dataframe(self, df, preceding_filters_column):
        return add_attribute_expression(df, self.attr_data)


class AttributeInput(Attribute):
    def get_dependencies(self, universe_attributes):
        return []

    def add_to_dataframe(self, df, preceding_filters_column):
        # Do nothing for input attribute
        return df
