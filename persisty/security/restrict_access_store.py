import dataclasses
from dataclasses import dataclass
from typing import Optional, List, Tuple

from persisty.errors import PersistyError
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.batch_edit import BatchEdit
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
            from persisty.security.store_security import StoreSecurity

            store_meta = self.store.get_meta()
            store_meta = dataclasses.replace(
                store_meta,
                store_security=StoreSecurity(
                    default_access=self.store_access, potential_access=self.store_access
                ),
            )
            setattr(self, "_store_meta", store_meta)
        return store_meta

    def filter_create(self, item: T) -> Optional[T]:
        if not self.store_access.creatable:
            raise PersistyError("unavailable_operation")
        if not self.store_access.create_filter.match(item, self.get_meta().attrs):
            raise PersistyError("create_forbidden")
        return item

    def update(
        self, updates: T, precondition: SearchFilterABC = INCLUDE_ALL
    ) -> Optional[T]:
        if not self.store_access.creatable:
            raise PersistyError("unavailable_operation")
        if self.store_access.delete_filter == INCLUDE_ALL:
            return self.store.update(updates, precondition)
        return super().update(updates, precondition)

    # noinspection PyUnusedLocal
    def filter_update(self, item: T, updates: T) -> T:
        if not self.store_access.creatable:
            raise PersistyError("unavailable_operation")
        item = dataclasses.replace(item, **dataclasses.asdict(updates))
        if not self.store_access.update_filter.match(item, self.get_meta().attrs):
            return
        return updates

    def filter_read(self, item: T) -> Optional[T]:
        if not self.store_access.readable:
            raise PersistyError("unavailable_operation")
        if self.store_access.read_filter.match(item, self.get_meta().attrs):
            return item

    # noinspection PyUnusedLocal
    def allow_delete(self, item: T) -> bool:
        if not self.store_access.deletable:
            raise PersistyError("unavailable_operation")
        return self.store_access.delete_filter.match(item, self.get_meta().attrs)

    def delete(self, key: str) -> bool:
        """Delete an stored from the data store. Return true if an item was deleted, false otherwise"""
        if not self.store_access.deletable:
            raise PersistyError("unavailable_operation")
        if self.store_access.delete_filter == INCLUDE_ALL:
            return self.store.delete(key)
        return super().delete(key)

    def filter_search_filter(
        self, search_filter: SearchFilterABC
    ) -> Tuple[SearchFilterABC, bool]:
        if not self.store_access.searchable:
            raise PersistyError("unavailable_operation")
        return search_filter & self.store_access.read_filter, True


def _check_edits(edits: List[BatchEdit], store_access: StoreAccess):
    for edit in edits:
        create_error = edit.create_item and not store_access.creatable
        update_error = edit.update_item and not store_access.updatable
        delete_error = edit.delete_key and not store_access.deletable
        if create_error or update_error or delete_error:
            raise PersistyError("unavailable_operation")


def restrict_access_store(store: StoreABC, store_access: StoreAccess):
    if store_access == ALL_ACCESS:
        return store
    return RestrictAccessStore(store, store_access)
