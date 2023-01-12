from abc import abstractmethod, ABC
from typing import Iterator

from marshy.factory.impl_marshaller_factory import get_impls

from persisty.secured.secured_store_factory_abc import SecuredStoreFactoryABC
from persisty.store.store_factory_abc import StoreFactoryABC


class StoreFactoryFinderABC(ABC):
    @abstractmethod
    def find_store_factories(self) -> Iterator[StoreFactoryABC]:
        """Find all available store items"""

    @abstractmethod
    def find_secured_store_factories(self) -> Iterator[StoreFactoryABC]:
        """Find all available store items"""


def find_store_factories() -> Iterator[StoreFactoryABC]:
    for finder in get_impls(StoreFactoryFinderABC):
        yield from finder().find_store_factories()


def find_secured_store_factories() -> Iterator[SecuredStoreFactoryABC]:
    for finder in get_impls(StoreFactoryFinderABC):
        yield from finder().find_secured_store_factories()
