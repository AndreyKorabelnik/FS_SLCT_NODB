from typing import List, Dict, Tuple


class Selection:
    def __init__(self, id: int, filters: List[Dict]):
        self.id = id
        self.filters = filters

    def get_application_levels(self) -> List[int]:
        return sorted(set(f['application_level'] for f in self.filters))

    def get_filters(self) -> List[Tuple]:
        return [(f['filter_id'], f['expression'], f['application_level']) for f in self.filters]

    def get_id(self) -> int:
        return self.id

    def get_filter_name(self, filter_id) -> str:
        return f"filter_{self.id}_{filter_id}"


def get_selections(selections: List[Dict]) -> List[Selection]:
    return [Selection(selection['selection_id'], selection['filters']) for selection in selections]
