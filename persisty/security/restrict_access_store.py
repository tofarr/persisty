import dataclasses
from dataclasses import dataclass
from typing import Optional, Tuple

from persisty.errors import PersistyError
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store.filtered_store_abc import FilteredStoreABC
from persisty.store.store_abc import StoreABC, T
from persisty.security.store_access import StoreAccess, ALL_ACCESS
from persisty.store_meta import StoreMeta


@dataclass
class RestrictAccessStore(FilteredStoreABC[T]):
    store: StoreABC[T]
    store_access: StoreAccess

    def get_store(self) -> StoreABC:
        return self.store

    def get_meta(self) -> StoreMeta:
        store_meta = getattr(self, "_store_meta", None)
        if not store_meta:
            store_meta = self.store.get_meta()
            store_meta = dataclasses.replace(
                store_meta,
                store_access=self.store_access & store_meta.store_access,
            )
            setattr(self, "_store_meta", store_meta)
        return store_meta

    def filter_create(self, item: T) -> Optional[T]:
        if not self.store_access.item_creatable(item, self.get_meta().attrs):
            raise PersistyError("create_forbidden")
        return item

    def update(
        self, updates: T, precondition: SearchFilterABC = INCLUDE_ALL
    ) -> Optional[T]:
        if self.store_access.update_filter is EXCLUDE_ALL:
            return
        return super().update(updates, precondition)

    # noinspection PyUnusedLocal
    def filter_update(self, item: T, updates: T) -> T:
        if self.store_access.item_updatable(item, updates, self.get_meta().attrs):
            return updates

    def read(self, key: str) -> Optional[T]:
        if self.store_access.read_filter is not EXCLUDE_ALL:
            return super().read(key)

    def filter_read(self, item: T) -> Optional[T]:
        if self.store_access.item_readable(item, self.get_meta().attrs):
            return item

    # noinspection PyUnusedLocal
    def allow_delete(self, item: T) -> bool:
        result = self.store_access.item_deletable(item, self.get_meta().attrs)
        return result

    def delete(self, key: str) -> bool:
        if self.store_access.delete_filter is EXCLUDE_ALL:
            return False
        return super().delete(key)

    def filter_search_filter(
        self, search_filter: SearchFilterABC
    ) -> Tuple[SearchFilterABC, bool]:
        if not self.store_access.searchable:
            return EXCLUDE_ALL
        return search_filter & self.store_access.read_filter, True

    def delete_all(self, search_filter: SearchFilterABC[T]):
        return self.store.delete_all(search_filter & self.store_access.delete_filter)


def restrict_access_store(store: StoreABC, store_access: StoreAccess):
    if store_access == ALL_ACCESS:
        return store
    return RestrictAccessStore(store, store_access)
