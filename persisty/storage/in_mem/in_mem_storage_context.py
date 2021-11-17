from dataclasses import dataclass, field
from typing import Union

from persisty.storage.in_mem.in_mem_meta_storage import InMemMetaStorage
from persisty.storage.storage_abc import StorageABC, T
from persisty.persisty_context_abc import StorageContextABC
from persisty.storage.storage_meta import StorageMeta


@dataclass(frozen=True)
class InMemStorageContext(StorageContextABC):
    meta_storage: InMemMetaStorage = field(default_factory=InMemMetaStorage)

    def get_storage(self, name: Union[str, T]) -> StorageABC[T]:
        return self.meta_storage.stores[name]

    def get_meta_storage(self) -> StorageABC[StorageMeta]:
        return self.meta_storage
