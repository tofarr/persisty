from abc import abstractmethod, ABC
from typing import Iterator

from marshy.factory.impl_marshaller_factory import get_impls

from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.store.store_abc import StoreABC


class StoreFactoryFinderABC(ABC):
    @abstractmethod
    def find_stores(self) -> Iterator[StoreABC]:
        """Find all available store items"""

    @abstractmethod
    def find_store_factories(self) -> Iterator[StoreFactoryABC]:
        """Find all available store items"""


def find_stores() -> Iterator[StoreABC]:
    names = set()
    for finder in get_impls(StoreFactoryFinderABC):
        for store in finder().find_stores():
            name = store.get_meta().name
            if name not in names:
                names.add(name)
                yield store


def find_store_factories() -> Iterator[StoreFactoryABC]:
    names = set()
    for finder in get_impls(StoreFactoryFinderABC):
        for factory in finder().find_store_factories():
            name = factory.get_meta().name
            if name not in names:
                names.add(name)
                yield factory
