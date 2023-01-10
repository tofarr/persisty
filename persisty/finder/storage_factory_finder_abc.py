from abc import abstractmethod, ABC
from typing import Iterator

from marshy.factory.impl_marshaller_factory import get_impls

from persisty.servey import SecuredStorageFactoryABC
from persisty.storage.storage_factory_abc import StorageFactoryABC


class StorageFactoryFinderABC(ABC):
    @abstractmethod
    def find_storage_factories(self) -> Iterator[StorageFactoryABC]:
        """Find all available storage items"""

    @abstractmethod
    def find_secured_storage_factories(self) -> Iterator[StorageFactoryABC]:
        """Find all available storage items"""


def find_storage_factories() -> Iterator[StorageFactoryABC]:
    for finder in get_impls(StorageFactoryFinderABC):
        yield from finder().find_storage_factories()


def find_secured_storage_factories() -> Iterator[SecuredStorageFactoryABC]:
    for finder in get_impls(StorageFactoryFinderABC):
        yield from finder().find_secured_storage_factories()
