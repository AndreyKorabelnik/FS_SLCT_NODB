from typing import List, Dict, Tuple


class Selection:
    def __init__(self, id: int, filters: List[Dict], output_attrs: List[Dict], output_settings: Dict):
        self.id = id
        self.filters = filters
        self.output_attrs = output_attrs
        self.output_settings = output_settings

    def get_application_levels(self) -> List[int]:
        return sorted(set(f['application_level'] for f in self.filters))

    def get_filters(self, application_level) -> List[Tuple]:
        return [(f['filter_id'], f['expression']) for f in self.filters
                if f['application_level'] == application_level]

    def get_output_attrs(self, application_level) -> List[str]:
        return [(f['attr_code']) for f in self.output_attrs
                if f['application_level'] == application_level]

    def get_output_settings(self) -> Tuple:
        return (self.output_settings['show_all'],
                self.output_settings['add_attributes'],
                self.output_settings['add_filters'],
                self.output_settings['add_failed_filters'])

    def get_id(self) -> int:
        return self.id


def get_selections(selections: List[Dict]) -> List[Selection]:
    return [Selection(selection['selection_id'],
                      selection['filters'],
                      selection['output_attrs'],
                      selection['output_settings'])
            for selection in selections]
