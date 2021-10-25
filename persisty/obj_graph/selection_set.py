from typing import Iterator, List, Optional


class SelectionSet:

    def __init__(self):
        self._fields = {}

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

    def get_selections(self, key: str) -> Optional['SelectionSet']:
        return self._fields.get(key)

    def has_sub_selections(self):
        return bool(self._fields)


def from_list(selection_set_list: List[str]) -> SelectionSet:
    root = SelectionSet()
    for selection_set in selection_set_list:
        path = selection_set.split('/').__iter__()
        root.add_selection(path)
    return root
