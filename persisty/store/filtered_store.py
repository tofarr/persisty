from typing import Optional, Tuple

from dataclasses import dataclass

from persisty.errors import PersistyError
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store.filtered_store_abc import FilteredStoreABC, T
from persisty.store.store_abc import StoreABC


@dataclass(frozen=True)
class FilteredStore(FilteredStoreABC[T]):
    store: StoreABC[T]
    search_filter: SearchFilterABC[T]

    def get_store(self) -> StoreABC:
        return self.store

    def filter_create(self, item: T) -> Optional[T]:
        if not self.search_filter.match(item, self.get_meta().attrs):
            raise PersistyError("create_forbidden")
        return item

    # noinspection PyUnusedLocal
    def filter_update(self, old_item: T, updates: T) -> T:
        # old_item has already been checked in read operation
        item = {**old_item, **updates}
        if not self.search_filter.match(item, self.get_meta().attrs):
            raise PersistyError("update_forbidden")
        return updates

    def filter_read(self, item: T) -> Optional[T]:
        if self.search_filter.match(item, self.get_meta().attrs):
            return item

    # noinspection PyUnusedLocal
    def allow_delete(self, item: T) -> bool:
        return self.search_filter.match(item, self.get_meta().attrs)

    def filter_search_filter(
        self, search_filter: SearchFilterABC
    ) -> Tuple[SearchFilterABC, bool]:
        return search_filter & self.search_filter, True
