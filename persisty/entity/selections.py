from typing import Iterator, List


class Selections:

    def __init__(self, **attrs):
        self._attrs = {**attrs} or {}

    def add_selection(self, path: Iterator[str]):
        selection_set = self
        updated = False
        while True:
            key = next(path, None)
            if not key:
                return updated
            field = selection_set._attrs.get(key)
            if not field:
                field = selection_set._attrs[key] = Selections()
                updated = True
            selection_set = field

    def get_selections(self, key: str) -> 'Selections':
        return self._attrs.get(key)

    def has_sub_selections(self):
        return bool(self._attrs)

    def __eq__(self, other):
        return self._attrs == getattr(other, '_attrs')


def from_selection_set_list(selection_set_list: List[str]) -> Selections:
    root = Selections()
    for selection_set in selection_set_list:
        path = selection_set.split('/').__iter__()
        root.add_selection(path)
    return root
