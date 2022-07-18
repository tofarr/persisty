from dataclasses import dataclass, fields
from typing import Tuple

from marshy.types import ExternalItemType

from persisty.access_control.access_control_abc import AccessControlABC
from persisty.search_filter.search_filter_abc import SearchFilterABC


@dataclass(frozen=True)
class AccessControl(AccessControlABC[ExternalItemType]):
    creatable: bool = False
    readable: bool = False
    updatable: bool = False
    deletable: bool = False
    searchable: bool = False

    def is_creatable(self, item: ExternalItemType) -> bool:
        return self.creatable

    def is_readable(self, item: ExternalItemType) -> bool:
        return self.readable

    def is_updatable(self, old_item: ExternalItemType, updates: ExternalItemType) -> bool:
        return self.updatable

    def is_deletable(self, item: ExternalItemType) -> bool:
        return self.deletable

    def is_searchable(self) -> bool:
        return self.searchable

    def transform_search_filter(self, search_filter: SearchFilterABC) -> Tuple[SearchFilterABC, bool]:
        return search_filter, True


ALL_ACCESS = AccessControl(**{f.name: True for f in fields(AccessControl)})
READ_ONLY = AccessControl(readable=True, searchable=True)
NO_ACCESS = AccessControl()
