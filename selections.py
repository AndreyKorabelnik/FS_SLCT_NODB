class Selection:
    def __init__(self, id, filters):
        self.id = id
        self.filters = filters

    def get_application_levels(self):
        return sorted(set(f['application_level'] for f in self.filters))

    def get_filters(self):
        return [(f['filter_id'], f['expression'], f['application_level']) for f in self.filters]

    def get_filter_name(self, filter_id):
        return f"filter_{self.id}_{filter_id}"


def get_selections(selections):
    return [Selection(selection['selection_id'], selection['filters']) for selection in selections]
