from abc import abstractmethod, ABC
from typing import Iterator

from marshy.factory.impl_marshaller_factory import get_impls

from persisty.store.store_factory_abc import StoreFactoryABC


class StorageFactoryFinderABC(ABC):
    @abstractmethod
    def find_storage_factories(self) -> Iterator[StoreFactoryABC]:
        """Find all available storage items"""


def find_storage_factories() -> Iterator[StoreFactoryABC]:
    for finder in get_impls(StorageFactoryFinderABC):
        yield from finder().find_storage_factories()
