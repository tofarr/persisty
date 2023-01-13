import dataclasses
from dataclasses import dataclass
from typing import Optional, Iterator, List, Dict

from persisty.errors import PersistyError
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.batch_edit import BatchEdit
from persisty.batch_edit_result import BatchEditResult
from persisty.result_set import ResultSet
from persisty.store.store_abc import StoreABC, T
from persisty.store_access import StoreAccess, ALL_ACCESS
from persisty.store_meta import StoreMeta


@dataclass
class RestrictAccessStore(StoreABC[T]):
    store: StoreABC[T]
    store_access: StoreAccess

    def get_store(self) -> StoreABC:
        return self.store

    def get_meta(self) -> StoreMeta:
        store_meta = getattr(self, "_store_meta", None)
        if not store_meta:
            store_meta = self.store.get_meta()
            store_meta = dataclasses.replace(
                store_meta, store_access=store_meta.store_access and self.store_access
            )
            setattr(self, "_store_meta", store_meta)
        return store_meta

    def get_store_access(self) -> StoreAccess:
        store_access = getattr(self, "_store_access", None)
        if not store_access:
            store_meta = self.store.get_meta()
            store_access = store_meta.store_access & self.store_access
            setattr(self, "_store_access", store_access)
        return store_access

    def create(self, item: T) -> Optional[T]:
        if not self.get_store_access().creatable:
            raise PersistyError("unavailable_operation")
        return self.store.create(item)

    def read(self, key: str) -> Optional[T]:
        if not self.get_store_access().readable:
            raise PersistyError("unavailable_operation")
        return self.store.read(key)

    def update(
        self, updates: T, precondition: SearchFilterABC = INCLUDE_ALL
    ) -> Optional[T]:
        if not self.get_store_access().updatable:
            raise PersistyError("unavailable_operation")
        return self.store.update(updates, precondition)

    def _update(self, key: str, item: T, updates: T) -> Optional[T]:
        if not self.get_store_access().updatable:
            raise PersistyError("unavailable_operation")
        return self.store._update(key, item, updates)

    def delete(self, key: str) -> bool:
        if not self.get_store_access().deletable:
            raise PersistyError("unavailable_operation")
        return self.store.delete(key)

    def _delete(self, key: str, item: T) -> bool:
        if not self.get_store_access().deletable:
            raise PersistyError("unavailable_operation")
        return self.store._delete(key, item)

    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        if not self.get_store_access().searchable:
            raise PersistyError("unavailable_operation")
        return self.store.count(search_filter)

    def search(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[T]:
        if not self.get_store_access().searchable:
            raise PersistyError("unavailable_operation")
        return self.store.search(search_filter, search_order, page_key, limit)

    def search_all(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
    ) -> Iterator[T]:
        if not self.get_store_access().searchable:
            raise PersistyError("unavailable_operation")
        return self.store.search_all(search_filter, search_order)

    def edit_batch(self, edits: List[BatchEdit]) -> List[BatchEditResult]:
        store_access = self.get_store_access()
        for edit in edits:
            if (
                (edit.create_item and not store_access.creatable)
                or (edit.update_item and not store_access.updatable)
                or (edit.delete_key and not store_access.deletable)
            ):
                raise PersistyError("unavailable_operation")
        return self.store.edit_batch(edits)

    def _edit_batch(
        self, edits: List[BatchEdit], items_by_key: Dict[str, T]
    ) -> List[BatchEditResult]:
        store_access = self.get_store_access()
        for edit in edits:
            if (
                (edit.create_item and not store_access.creatable)
                or (edit.update_item and not store_access.updatable)
                or (edit.delete_key and not store_access.deletable)
            ):
                raise PersistyError("unavailable_operation")
        return self.store._edit_batch(edits, items_by_key)


def restrict_access_store(store: StoreABC, store_access: StoreAccess) -> StoreABC:
    if store_access == ALL_ACCESS:
        return store
    return RestrictAccessStore(store, store_access)
