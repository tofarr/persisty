from abc import abstractmethod, ABC
from typing import Iterator

from marshy.factory.impl_marshaller_factory import get_impls

from persisty.store_meta import StoreMeta, get_meta


class StoredFinderABC(ABC):
    @abstractmethod
    def find_stored(self) -> Iterator[StoreMeta]:
        """Find all available store items"""


def find_stored() -> Iterator[StoreMeta]:
    names = set()
    for finder in get_impls(StoredFinderABC):
        for store_meta in finder().find_stored():
            name = store_meta.name
            if name not in names:
                names.add(name)
                yield store_meta
