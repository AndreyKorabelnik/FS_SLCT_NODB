from typing import List, Dict, Tuple
import sql_expr_parser


class Attribute:
    def __init__(self, code: str, data_type: str):
        self.code = code
        self.data_type = data_type

    def get_dependencies(self) -> List[str]:
        raise NotImplementedError("Subclasses should implement this method.")

    def get_sql_expression(self, preceding_filters: List = None) -> str:
        raise NotImplementedError("Subclasses should implement this method.")


class AttributeRank(Attribute):
    def __init__(self, code: str, data_type: str, rank_attrs: List[str], partition_by: str = None):
        super().__init__(code, data_type)
        self.rank_attrs = rank_attrs
        self.partition_by = partition_by

    def get_dependencies(self) -> List[str]:
        r = [a['attr_code'] for a in self.rank_attrs]
        if self.partition_by:
            r.append(self.partition_by)
        return r

    def _get_rank_attrs(self, preceding_filters: List = None) -> List[Tuple]:
        # apply preceding filters first. it ranks DESC to give rows passed filters more priority
        rank_attrs = [(preceding_filter, 'DESC') for preceding_filter in preceding_filters]
        rank_attrs.extend((a['attr_code'], a['direction'])
                          for a in sorted(self.rank_attrs, key=lambda x: x['order']))
        return rank_attrs

    def get_sql_expression(self, preceding_filters: List = None) -> str:
        rank_attrs = ','.join(f"{attr_code} {direction}"
                              for attr_code, direction in self._get_rank_attrs(preceding_filters))
        partition_by_string = f'partition by {self.partition_by}' if self.partition_by else ''
        return f"rank() over({partition_by_string} order by {rank_attrs} nulls last) as {self.code}"


class AttributeAggregate(Attribute):
    def __init__(self, code: str, data_type: str, aggregate_attr_code: str, aggregate_function: str,
                 aggregate_direction: str, partition_by: str = None):
        super().__init__(code, data_type)
        self.aggregate_attr_code = aggregate_attr_code
        self.aggregate_function = aggregate_function
        self.aggregate_direction = aggregate_direction
        self.partition_by = partition_by

    def get_dependencies(self) -> List[str]:
        r = [self.aggregate_attr_code]
        if self.partition_by:
            r.append(self.partition_by)
        return r

    def get_sql_expression(self, preceding_filters: List = None) -> str:
        if preceding_filters:
            aux_string = " and ".join(f"{preceding_filter}=1" for preceding_filter in preceding_filters)
            aggregate_expression = f'(case when {aux_string} then {self.aggregate_attr_code} end)'
        else:
            aggregate_expression = self.aggregate_attr_code
        sql_expression = f'{self.aggregate_function}({aggregate_expression}) over ({self.partition_by})'
        if self.aggregate_direction:
            sql_expression += f'order by {self.aggregate_attr_code} {self.aggregate_direction}'
        return f"{sql_expression} as {self.code}"


class AttributeExpression(Attribute):
    def __init__(self, code: str, data_type: str, expression: str):
        super().__init__(code, data_type)
        self.expression = expression

    def get_dependencies(self) -> List[str]:
        return sql_expr_parser.extract_identifiers(self.expression)

    def get_sql_expression(self, preceding_filters: List = None) -> str:
        return f"{self.expression} as {self.code}"


class AttributeInput(Attribute):
    def get_dependencies(self) -> List[str]:
        return []

    def get_sql_expression(self, preceding_filters: List = None) -> str:
        return self.code


def get_universe_attributes(universe: List[Dict]) -> List[Attribute]:
    universe_attributes = list()
    for attr in universe:
        partition_by = attr.get('partition_by')
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


def get_attribute(attr_code: str, universe_attributes: List[Attribute]) -> Attribute:
    """
    Returns Attribute type by attr_code from universe_attributes
    """
    try:
        return next(attr for attr in universe_attributes if attr.code == attr_code)
    except StopIteration as e:
        raise Exception(f'Attribute {attr_code} not found in universe: {e}')


def get_attribute_dependencies(attr_code: str, universe_attributes: List[Attribute]) -> List[str]:
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
