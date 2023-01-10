from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from servey.security.authorization import Authorization

from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta


@dataclass
class SecuredStorageFactoryABC(ABC):
    """
    Factory for storage objects which allows defining access control rules for storage.
    """

    @abstractmethod
    def get_storage_meta(self) -> StorageMeta:
        """ Get the meta for the storage """

    @abstractmethod
    def create(self, authorization: Optional[Authorization]) -> Optional[StorageABC]:
        """ Create a new storage instance """
