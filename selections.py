from typing import List, Dict, Tuple


class Selection:
    def __init__(self, id: int, filters: List[Dict], output: Dict):
        self.id = id
        self.filters = filters
        self.output = output

    def get_application_levels(self) -> List[int]:
        return sorted(set(f['application_level'] for f in self.filters))

    def get_filters(self) -> List[Tuple]:
        return [(f['filter_id'], f['expression'], f['application_level']) for f in self.filters]

    def get_filters(self, application_level) -> List[Tuple]:
        return [(f['filter_id'], f['expression']) for f in self.filters
                if f['application_level'] == application_level]

    def get_output_settings(self) -> Tuple:
        return self.output['show_all'], self.output['add_attributes'], self.output['add_filters']

    def get_id(self) -> int:
        return self.id



def get_selections(selections: List[Dict]) -> List[Selection]:
    return [Selection(selection['selection_id'], selection['filters'], selection['output']) for selection in selections]
