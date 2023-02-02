from abc import abstractmethod, ABC
from typing import Iterator, Dict

from marshy.factory.impl_marshaller_factory import get_impls

from persisty_data.data_store_abc import DataStoreABC


class DataStoreFinderABC(ABC):
    @abstractmethod
    def find_data_stores(self) -> Iterator[DataStoreABC]:
        """Find all available store items"""


def find_data_stores() -> Iterator[DataStoreABC]:
    for finder in get_impls(DataStoreFinderABC):
        yield from finder().find_data_stores()


def add_actions_for_all_data_stores(target: Dict):
    for data_store_factory in find_data_stores():
        for action in data_store_factory.create_actions():
            target[action.name] = action.fn
