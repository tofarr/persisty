import dataclasses
from dataclasses import dataclass
from typing import Optional, Iterator, List, Dict

from marshy.types import ExternalItemType

from persisty.errors import PersistyError
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.storage.batch_edit import BatchEdit
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.result_set import ResultSet
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_access import StorageAccess, ALL_ACCESS
from persisty.storage.storage_meta import StorageMeta


@dataclass
class RestrictAccessStorage(StorageABC):
    storage: StorageABC
    storage_access: StorageAccess

    def get_storage(self) -> StorageABC:
        return self.storage

    def get_storage_meta(self) -> StorageMeta:
        storage_meta = getattr(self, '_storage_meta', None)
        if not storage_meta:
            storage_meta = self.storage.get_storage_meta()
            storage_meta = dataclasses.replace(
                storage_meta,
                storage_access=storage_meta.storage_access and self.storage_access
            )
            setattr(self, '_storage_meta', storage_meta)
        return storage_meta

    def get_storage_access(self) -> StorageAccess:
        storage_access = getattr(self, '_storage_access', None)
        if not storage_access:
            storage_meta = self.storage.get_storage_meta()
            storage_access = storage_meta.storage_access & self.storage_access
            setattr(self, '_storage_access', storage_access)
        return storage_access

    def create(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        if not self.get_storage_access().creatable:
            raise PersistyError('unavailable_operation')
        return self.storage.create(item)

    def read(self, key: str) -> Optional[ExternalItemType]:
        if not self.get_storage_access().readable:
            raise PersistyError('unavailable_operation')
        return self.storage.read(key)

    def update(
        self, updates: ExternalItemType, precondition: SearchFilterABC = INCLUDE_ALL
    ) -> Optional[ExternalItemType]:
        if not self.get_storage_access().updatable:
            raise PersistyError('unavailable_operation')
        return self.storage.update(updates, precondition)

    def _update(
        self,
        key: str,
        item: ExternalItemType,
        updates: ExternalItemType,
        precondition: SearchFilterABC = INCLUDE_ALL,
    ) -> Optional[ExternalItemType]:
        if not self.get_storage_access().updatable:
            raise PersistyError('unavailable_operation')
        return self.storage._update(key, item, updates, precondition)

    def delete(self, key: str) -> bool:
        if not self.get_storage_access().deletable:
            raise PersistyError('unavailable_operation')
        return self.storage.delete(key)

    def _delete(self, key: str, item: ExternalItemType) -> bool:
        if not self.get_storage_access().deletable:
            raise PersistyError('unavailable_operation')
        return self.storage._delete(key, item)

    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        if not self.get_storage_access().searchable:
            raise PersistyError('unavailable_operation')
        return self.storage.count(search_filter)

    def search(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[ExternalItemType]:
        if not self.get_storage_access().searchable:
            raise PersistyError('unavailable_operation')
        return self.storage.search(search_filter, search_order, page_key, limit)

    def search_all(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
    ) -> Iterator[ExternalItemType]:
        if not self.get_storage_access().searchable:
            raise PersistyError('unavailable_operation')
        return self.storage.search_all(search_filter, search_order)

    def edit_batch(self, edits: List[BatchEdit]) -> List[BatchEditResult]:
        storage_access = self.get_storage_access()
        for edit in edits:
            if (
                (edit.create_item and not storage_access.creatable)
                or (edit.update_item and not storage_access.updatable)
                or (edit.delete_key and not storage_access.deletable)
            ):
                raise PersistyError('unavailable_operation')
        return self.storage.edit_batch(edits)

    def _edit_batch(
            self, edits: List[BatchEdit], items_by_key: Dict[str, ExternalItemType]
    ) -> List[BatchEditResult]:
        storage_access = self.get_storage_access()
        for edit in edits:
            if (
                (edit.create_item and not storage_access.creatable)
                or (edit.update_item and not storage_access.updatable)
                or (edit.delete_key and not storage_access.deletable)
            ):
                raise PersistyError('unavailable_operation')
        return self.storage._edit_batch(edits, items_by_key)


def restrict_access_storage(storage: StorageABC, storage_access: StorageAccess) -> StorageABC:
    if storage_access == ALL_ACCESS:
        return storage
    return RestrictAccessStorage(storage, storage_access)
