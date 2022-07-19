from dataclasses import dataclass, fields

from persisty.access_control.access_control_abc import AccessControlABC
from persisty.access_control.obj_access_control_abc import ObjAccessControlABC


@dataclass(frozen=True)
class AccessControl(AccessControlABC, ObjAccessControlABC):
    creatable: bool = False
    readable: bool = False
    updatable: bool = False
    deletable: bool = False
    searchable: bool = False

    def is_creatable(self, item) -> bool:
        return self.creatable

    def is_readable(self, item) -> bool:
        return self.readable

    def is_updatable(self, old_item, updates) -> bool:
        return self.updatable

    def is_deletable(self, item) -> bool:
        return self.deletable

    def is_searchable(self) -> bool:
        return self.searchable

    def transform_search_filter(self, search_filter):
        return search_filter, True
