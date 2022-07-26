from typing import Optional, Tuple

from dataclasses import dataclass
from marshy.types import ExternalItemType

from persisty.errors import PersistyError
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.filtered_storage_abc import FilteredStorageABC
from persisty.storage.storage_abc import StorageABC


@dataclass(frozen=True)
class FilteredStorage(FilteredStorageABC):
    storage: StorageABC
    search_filter: SearchFilterABC

    def get_storage(self) -> StorageABC:
        return self.storage

    def filter_create(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        if not self.search_filter.match(item, self.get_storage_meta().fields):
            raise PersistyError("create_forbidden")
        return item

    # noinspection PyUnusedLocal
    def filter_update(
        self, old_item: ExternalItemType, updates: ExternalItemType
    ) -> ExternalItemType:
        # old_item has already been checked in read operation
        item = {**old_item, **updates}
        if not self.search_filter.match(item, self.get_storage_meta().fields):
            raise PersistyError("update_forbidden")
        return updates

    def filter_read(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        if self.search_filter.match(item):
            return item

    # noinspection PyUnusedLocal
    def allow_delete(self, item: ExternalItemType) -> bool:
        return self.search_filter.match(item)

    def filter_search_filter(
        self, search_filter: SearchFilterABC
    ) -> Tuple[SearchFilterABC, bool]:
        return search_filter & self.search_filter, True
