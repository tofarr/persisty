from abc import ABC, abstractmethod
from typing import Optional

from servey.security.authorization import Authorization

from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta


class StorageFactoryABC(ABC):
    @abstractmethod
    def get_storage_meta(self) -> StorageMeta:
        """Get the storage meta"""

    @abstractmethod
    def create(self, authorization: Optional[Authorization]) -> Optional[StorageABC]:
        """Create a new storage object with the authorization given"""
