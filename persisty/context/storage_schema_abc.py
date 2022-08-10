from abc import abstractmethod, ABC
from typing import Optional, Iterator

from persisty.access_control.authorization import Authorization
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta


class StorageSchemaABC(ABC):
    priority: int = 100

    @abstractmethod
    def get_name(self) -> str:
        """Get the name of this factory"""

    @abstractmethod
    def create_storage(
        self,
        storage_meta: StorageMeta,
        authorization: Authorization,
    ) -> Optional[StorageABC]:
        """Create a storage object for the meta given if possible."""

    @abstractmethod
    def get_storage_by_name(
        self, storage_name: str, authorization: Authorization
    ) -> Optional[StorageABC]:
        """Get a storage object based upon its name"""

    @abstractmethod
    def get_all_storage_meta(self) -> Iterator[StorageMeta]:
        """Get the meta for this factory"""
