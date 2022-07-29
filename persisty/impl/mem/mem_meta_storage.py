from dataclasses import field, dataclass
from typing import Optional, Dict, List, Iterator

from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.types import ExternalItemType

from persisty.context.meta_storage_abc import (
    MetaStorageABC,
    STORAGE_META_MARSHALLER,
    STORED_STORAGE_META,
)
from persisty.impl.mem.mem_storage import mem_storage, MemStorage
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.batch_edit import BatchEditABC, Update, Delete
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta
from persisty.storage.wrapper_storage_abc import WrapperStorageABC


@dataclass
class MemMetaStorage(MetaStorageABC, WrapperStorageABC):
    """
    Meta storage which relies on another storage object and dynamically initializes them as requested
    """

    meta_storage: StorageABC = field(
        default_factory=lambda: mem_storage(STORED_STORAGE_META)
    )
    item_storage: Dict[str, StorageABC] = field(default_factory=dict)
    storage_meta_marshaller: MarshallerABC[StorageMeta] = field(
        default=STORAGE_META_MARSHALLER
    )

    def storage_from_meta(self, storage_meta: StorageMeta):
        return mem_storage(storage_meta)

    def get_item_storage(self, name: str) -> Optional[StorageABC]:
        storage = self.item_storage.get(name)
        if storage:
            return storage
        storage_meta = self.meta_storage.read(name)
        if storage_meta:
            storage_meta = self.storage_meta_marshaller.load(storage_meta)
            storage = self.storage_from_meta(storage_meta)
            self.item_storage[storage_meta.name] = storage
            return storage

    def get_storage(self) -> StorageABC:
        return self.meta_storage

    def update(
        self, updates: ExternalItemType, search_filter: SearchFilterABC = INCLUDE_ALL
    ) -> Optional[ExternalItemType]:
        new_item = self.get_storage().update(updates, search_filter)
        return new_item

    def after_update(self, new_item: ExternalItemType):
        if new_item:
            storage_meta = self.storage_meta_marshaller.load(new_item)
            storage = self.item_storage.get(storage_meta.name)
            if storage:
                storage = _unnest(storage)
                storage.storage_meta = storage_meta

    def delete(self, key: str) -> bool:
        result = self.get_storage().delete(key)
        if result:
            self.after_delete(key)
        return result

    def after_delete(self, key: str):
        storage = self.item_storage.pop(key, None)
        if storage:
            storage = _unnest(storage)
            storage.items = None  # Trash the storage to prevent future use

    def edit_batch(self, edits: List[BatchEditABC]):
        results = self.get_storage().edit_batch(edits)
        for result in results:
            self.after_batch_edit(result)
        return results

    def edit_all(self, edits: Iterator[BatchEditABC]) -> Iterator[BatchEditResult]:
        for result in self.get_storage().edit_all(edits):
            self.after_batch_edit(result)
            yield result

    def after_batch_edit(self, result: BatchEditResult):
        if result.success:
            if isinstance(result.edit, Update):
                self.after_update(result.edit.updates)
            elif isinstance(result.edit, Delete):
                self.after_delete(result.edit.key)


def _unnest(storage: StorageABC) -> MemStorage:
    while hasattr(storage, "storage"):  # Un-nest to get MemStorage instance...
        storage = storage.storage
    return storage
