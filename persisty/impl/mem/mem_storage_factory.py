from dataclasses import dataclass, field
from typing import Optional

from marshy.types import ExternalItemType
from servey.security.authorization import Authorization

from persisty.impl.mem.mem_storage import MemStorage
from persisty.storage.schema_validating_storage import SchemaValidatingStorage
from persisty.storage.secured_storage import secured_storage
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_factory_abc import StorageFactoryABC
from persisty.storage.storage_meta import StorageMeta
from persisty.trigger.wrapper import triggered_storage


@dataclass
class MemStorageFactory(StorageFactoryABC):
    storage_meta: StorageMeta
    items: ExternalItemType = field(default_factory=dict)

    def get_storage_meta(self) -> StorageMeta:
        return self.storage_meta

    def create(self, authorization: Optional[Authorization]) -> Optional[StorageABC]:
        storage = MemStorage(self.storage_meta, self.items)
        storage = SchemaValidatingStorage(storage)
        storage = secured_storage(storage, authorization)
        storage = triggered_storage(storage)
        return storage
