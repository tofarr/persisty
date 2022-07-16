from dataclasses import dataclass
from typing import Optional, List, Tuple

from marshy.types import ExternalItemType

from persisty.storage.access_control.access_control import NO_ACCESS
from persisty.storage.batch_edit import Update, Create, Delete, BatchEditABC
from persisty.storage.filtered_storage import FilteredStorage
from persisty.storage.result_set import ResultSet
from persisty.storage.search_filter.include_all import INCLUDE_ALL
from persisty.storage.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.search_order import SearchOrder, NO_ORDER
from persisty.storage.storage_meta import StorageMeta


class SecurityError(Exception):
    pass


class SecuredStorage(FilteredStorage):

    def filter_create(self, item: ExternalItemType) -> ExternalItemType:
        if self.storage_meta.access_control.is_creatable(item):
            return item

    def filter_update(self, item: ExternalItemType, updates: ExternalItemType) -> ExternalItemType:
        if self.storage_meta.access_control.is_updatable(item):
            return item

    def filter_read(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        if self.storage_meta.access_control.is_readable(item):
            return item

    def allow_delete(self, item: ExternalItemType) -> bool:
        return self.storage_meta.access_control.is_deletable(item)

    def filter_search_filter(self, search_filter: SearchFilterABC) -> Tuple[SearchFilterABC, bool]:
        access_control = self.storage_meta.access_control
        if not access_control.is_searchable():
            return NO_ACCESS, True
        return access_control.transform_search_filter(search_filter)
