import sql_expr_parser


class Attribute:
    def __init__(self, code, data_type):
        self.code = code
        self.data_type = data_type

    def get_dependencies(self):
        raise NotImplementedError("Subclasses should implement this method.")

    def add_to_dataframe(self, df, preceding_filters_column=None):
        raise NotImplementedError("Subclasses should implement this method.")


class AttributeRank(Attribute):
    def __init__(self, code, data_type, rank_attrs, partition_by=None):
        super().__init__(code, data_type)
        self.rank_attrs = rank_attrs
        self.partition_by = partition_by

    def get_dependencies(self):
        r = [a['attr_code'] for a in self.rank_attrs]
        if self.partition_by:
            r.append(self.partition_by)
        return r

    def add_to_dataframe(self, df, preceding_filters_column=None):
        rank_attrs = [(a['attr_code'], a['direction'])
                      for a in sorted(self.rank_attrs, key=lambda x: x['order'])]
        if preceding_filters_column:
            # apply preceding filters first. it ranks DESC to make rows passed filters more priority
            rank_attrs.insert(0, (preceding_filters_column, 'DESC'))
        combine_rank_attr = f"{self.code}_aux"
        df[combine_rank_attr] = df.apply(lambda row: tuple((-1 if direction == 'DESC' else 1) * row[a]
                                                           for (a, direction) in rank_attrs), axis=1)
        if self.partition_by:
            ranks = df.groupby(self.partition_by)[combine_rank_attr].rank(method='first')
        else:
            ranks = df[combine_rank_attr].rank(method='first')
        # without reseting all values are NaN in df
        ranks.reset_index(drop=True, inplace=True)
        df[self.code] = ranks
        return df


class AttributeAggregate(Attribute):
    def __init__(self, code, data_type, aggregate_attr_code, aggregate_function, aggregate_direction,
                 partition_by=None):
        super().__init__(code, data_type)
        self.aggregate_attr_code = aggregate_attr_code
        self.aggregate_function = aggregate_function
        self.aggregate_direction = aggregate_direction
        self.partition_by = partition_by

    def get_dependencies(self):
        r = [self.aggregate_attr_code]
        if self.partition_by:
            r.append(self.partition_by)
        return r

    def add_to_dataframe(self, df, preceding_filters_column=None):
        aggr_attr = self.aggregate_attr_code
        if preceding_filters_column:
            # apply preceding filters first
            df[f'{aggr_attr}_aux'] = df.apply(
                lambda row: row[aggr_attr] if row[preceding_filters_column] else 0,
                axis=1)
            aggr_attr = f'{aggr_attr}_aux'
        if self.partition_by:
            df[self.code] = df.groupby(self.partition_by)[aggr_attr].apply(self.aggregate_function)
        else:
            df[self.code] = df[aggr_attr].apply(self.aggregate_function)
        return df


class AttributeExpression(Attribute):
    def __init__(self, code, data_type, expression):
        super().__init__(code, data_type)
        self.expression = expression

    def add_to_dataframe(self, df, preceding_filters_column=None):
        df[self.code] = eval(sql_expr_parser.transform_to_pandas(self.expression))
        return df

    def get_dependencies(self):
        return sql_expr_parser.extract_identifiers(sql_expr_parser.parse(self.expression))


class AttributeInput(Attribute):
    def get_dependencies(self):
        return []

    def add_to_dataframe(self, df, preceding_filters_column=None):
        # Do nothing for input attribute
        return df


def get_universe_attributes(universe):
    universe_attributes = list()
    for attr in universe:
        partition_by = attr['partition_by'] if 'partition_by' in attr and attr['partition_by'] else None
        if attr['attr_type'] == 'INPUT':
            a = AttributeInput(attr['attr_code'],
                               attr['attr_data_type'])
        elif attr['attr_type'] == 'RANK':
            a = AttributeRank(attr['attr_code'],
                              attr['attr_data_type'],
                              attr['rank_attrs'],
                              partition_by)
        elif attr['attr_type'] == 'AGGREGATE':
            a = AttributeAggregate(attr['attr_code'],
                                   attr['attr_data_type'],
                                   attr['aggregate_attr_code'],
                                   attr['aggregate_function'],
                                   attr['aggregate_direction'],
                                   partition_by)
        elif attr['attr_type'] == 'EXPRESSION':
            a = AttributeExpression(attr['attr_code'],
                                    attr['attr_data_type'],
                                    attr['expression'])
        universe_attributes.append(a)
    return universe_attributes