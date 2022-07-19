from dataclasses import field, dataclass
from typing import Optional, Dict

from persisty.access_control.obj_access_control_abc import ObjAccessControlABC
from persisty.context.meta_storage import meta_result_set
from persisty.context.obj_storage_meta import MetaStorageABC, CreateStorageMetaInput, UpdateStorageMetaInput, \
    StorageMetaSearchFilter, StorageMetaSearchOrder
from persisty.errors import PersistyError
from persisty.impl.mem.mem_storage import MemStorage
from persisty.storage.result_set import ResultSet
from persisty.storage.storage_meta import StorageMeta
from persisty.util import dataclass_to_params


@dataclass
class MemMetaStorage(MetaStorageABC):
    access_control: ObjAccessControlABC[StorageMeta, StorageMetaSearchFilter, CreateStorageMetaInput,
                                        UpdateStorageMetaInput]
    storage: Dict[str, MemStorage] = field(default_factory=dict)

    @property
    def batch_size(self) -> int:
        return 100

    def create(self, item: CreateStorageMetaInput) -> StorageMeta:
        if self.access_control.is_creatable(item):
            raise PersistyError('create_forbidden')
        params = dataclass_to_params(item)
        storage_meta = StorageMeta(**params)
        if storage_meta.name in self.storage:
            raise PersistyError(f'existing_value:{storage_meta.name}')
        storage = MemStorage(storage_meta)
        self.storage[storage_meta.name] = storage
        return storage_meta

    def read(self, key: str) -> Optional[StorageMeta]:
        storage = self.get_storage().get(key)
        if not storage or not self.access_control.is_readable(storage.storage_meta):
            return None
        return storage.storage_meta

    def update(self, updates: UpdateStorageMetaInput) -> Optional[StorageMeta]:
        storage = self.get_storage().get(updates.name)
        if not storage or self.access_control.is_updatable(storage.storage_meta, updates):
            return None
        params = dataclass_to_params(storage.storage_meta)
        params.update(**dataclass_to_params(updates))
        storage_meta = StorageMeta(**params)
        storage.storage_meta = storage_meta  # TOOD: What happens if we add a new non nullable field?
        return storage_meta

    def delete(self, key: str) -> bool:
        return bool(self.get_storage().pop(key, None))

    def search(self,
               search_filter_factory: Optional[StorageMetaSearchFilter] = None,
               search_order_factory: Optional[StorageMetaSearchOrder] = None,
               page_key: Optional[str] = None,
               limit: Optional[int] = None
               ) -> ResultSet[StorageMeta]:
        search_filter_factory = self.access_control.transform_search_filter(search_filter_factory)
        items = iter(self.get_storage().values())
        items = (i.storage_meta for i in items if self.access_control.is_readable(i.storage_meta))
        return meta_result_set(items, search_filter_factory, search_order_factory, page_key, limit)

    def count(self, search_filter: Optional[StorageMetaSearchFilter] = None) -> int:
        if not search_filter:
            return len(self.storage)
        count = sum(1 for _ in self.search_all(search_filter))
        return count
