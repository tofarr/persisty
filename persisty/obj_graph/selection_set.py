from typing import Iterator, List


class SelectionSet:

    def __init__(self, **fields):
        self._fields = {**fields} or {}

    """
    def __init__(self, fields: Dict[str, ForwardRef('persisty.obj_graph.SelectionSet')] = None):
        self._fields = fields or {}
    """

    def add_selection(self, path: Iterator[str]):
        selection_set = self
        updated = False
        while True:
            key = next(path, None)
            if not key:
                return updated
            field = selection_set._fields.get(key)
            if not field:
                field = selection_set._fields[key] = SelectionSet()
                updated = True
            selection_set = field

    def get_selections(self, key: str) -> 'SelectionSet':
        return self._fields.get(key)

    def has_sub_selections(self):
        return bool(self._fields)

    def __eq__(self, other):
        return self._fields == getattr(other, '_fields')


def from_selection_set_list(selection_set_list: List[str]) -> SelectionSet:
    root = SelectionSet()
    for selection_set in selection_set_list:
        path = selection_set.split('/').__iter__()
        root.add_selection(path)
    return root
