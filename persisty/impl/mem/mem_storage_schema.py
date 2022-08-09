from dataclasses import field, dataclass
from typing import Optional, Dict, Iterator

from persisty.access_control.authorization import Authorization
from persisty.access_control.constants import ALL_ACCESS
from persisty.context.storage_schema_abc import StorageSchemaABC
from persisty.errors import PersistyError
from persisty.impl.mem.mem_storage import MemStorage
from persisty.storage.schema_validating_storage import SchemaValidatingStorage
from persisty.storage.secured_storage import SecuredStorage
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta


@dataclass
class MemStorageSchema(StorageSchemaABC):
    """ Storage factory for in memory storage. Saves a cache of storage items for reuse. """
    name: str = 'mem'
    storage: Dict[str, StorageABC] = field(default_factory=dict)

    def get_name(self) -> str:
        return self.name

    def create_storage(self, storage_meta: StorageMeta, authorization: Authorization) -> Optional[StorageABC]:
        storage = self.storage.get(storage_meta.name)
        if storage:
            if storage.get_storage_meta() != storage_meta:
                raise PersistyError(f'storage_already_exists:{storage_meta.name}')
            else:
                storage = MemStorage(storage_meta)
                storage = SchemaValidatingStorage(storage, storage_meta.to_schema())
                self.storage[storage_meta.name] = storage
        access_control = storage_meta.get_access_control(authorization)
        if access_control != ALL_ACCESS:
            storage = SecuredStorage(storage, access_control)
        return storage

    def get_storage_by_name(self, storage_name: str, authorization: Authorization) -> Optional[StorageABC]:
        storage = self.storage.get(storage_name)
        if not storage:
            return
        access_control = storage.get_storage_meta().get_access_control(authorization)
        if access_control != ALL_ACCESS:
            storage = SecuredStorage(storage, access_control)
        return storage

    def get_all_storage_meta(self) -> Iterator[StorageMeta]:
        for storage in self.storage.values():
            yield storage.get_storage_meta()
