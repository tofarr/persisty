from abc import abstractmethod
from typing import Tuple

from dataclasses import dataclass
from marshy.types import ExternalItemType

from persisty.access_control.constants import ALL_ACCESS, NO_ACCESS
from persisty.access_control.access_control_abc import AccessControlABC
from persisty.storage.field.field_filter import FieldFilter
from persisty.search_filter.search_filter_abc import SearchFilterABC


@dataclass(frozen=True)
class FieldFilterAccessControl(AccessControlABC):
    field_filter: FieldFilter
    match_access: AccessControlABC = ALL_ACCESS
    no_match_access: AccessControlABC = NO_ACCESS

    def access_control_for(self, item: ExternalItemType) -> AccessControlABC:
        if self.field_filter.match(item):
            return self.match_access
        return self.no_match_access

    def is_creatable(self, item: ExternalItemType) -> bool:
        return self.access_control_for(item).is_creatable(item)

    def is_readable(self, item: ExternalItemType) -> bool:
        return self.access_control_for(item).is_readable(item)

    def is_updatable(
        self, old_item: ExternalItemType, updates: ExternalItemType
    ) -> bool:
        return self.access_control_for(old_item).is_updatable(old_item, updates)

    def is_deletable(self, item: ExternalItemType) -> bool:
        return self.access_control_for(item).is_deletable(item)

    @abstractmethod
    def is_searchable(self) -> bool:
        return self.match_access.is_searchable() or self.no_match_access.is_searchable()

    @abstractmethod
    def transform_search_filter(
        self, search_filter: SearchFilterABC
    ) -> Tuple[SearchFilterABC, bool]:
        match_filter, match_handled = self.match_access.transform_search_filter(
            search_filter
        )
        (
            no_match_filter,
            no_match_handled,
        ) = self.no_match_access.transform_search_filter(search_filter)
        if match_handled and no_match_handled:
            if self._is_simple_non_read(self.match_access):
                return no_match_filter | ~self.field_filter, True
            if self._is_simple_non_read(self.no_match_access):
                return match_filter | self.field_filter, True
        return match_filter | no_match_filter, False

    @staticmethod
    def _is_simple_non_read(access_control: AccessControlABC):
        return (
            hasattr(access_control, "readable")
            and getattr(access_control, "readable") is False
        )
