from dataclasses import dataclass
from typing import Optional

from servey.security.authorization import Authorization

from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_factory_abc import StorageFactoryABC
from persisty.storage.storage_meta import StorageMeta


@dataclass
class HybridStorageFactory(StorageFactoryABC):
    storage_meta: StorageMeta

    def get_storage_meta(self) -> StorageMeta:
        pass

    def create(self, authorization: Optional[Authorization]) -> Optional[StorageABC]:
        # If we have SQL libraries and SQL settings - create an sql storage. (if we are in debug mode, check and create)
        # If we are in the lambda environment, create dynamodb storage (if we are in debug mode, check and create)
        # Otherwise, use memory storage!

