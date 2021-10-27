from abc import abstractmethod
from typing import Optional, Iterator, Type, Any

from persisty.capabilities import Capabilities
from persisty.edit import Edit
from persisty.page import Page
from persisty.store.store_abc import T, StoreABC


class WrapperStoreABC(StoreABC[T]):

    @property
    @abstractmethod
    def store(self) -> StoreABC[T]:
        """ Get the wrapped store """

    @property
    def name(self) -> str:
        return self.store.name

    @property
    def item_type(self) -> Type[T]:
        return self.store.item_type

    @property
    def capabilities(self) -> Capabilities:
        return self.store.capabilities

    def get_key(self, item: T) -> str:
        return self.store.get_key(item)

    def create(self, item: T) -> str:
        return self.store.create(item)

    def read(self, key: str) -> Optional[T]:
        return self.store.read(key)

    def read_all(self, keys: Iterator[str], error_on_missing: bool = True) -> Iterator[T]:
        return self.store.read_all(keys, error_on_missing)

    def update(self, item: T) -> T:
        return self.store.update(item)

    def destroy(self, key: str) -> bool:
        return self.store.destroy(key)

    def search(self, search_filter: Any = None) -> Iterator[T]:
        return self.store.search(search_filter)

    def count(self, search_filter: Any = None) -> int:
        return self.store.count(search_filter)

    def paged_search(self, search_filter: Any = None, page_key: str = None, limit: int = 20) -> Page[T]:
        return self.store.paged_search(search_filter, page_key, limit)

    def edit_all(self, edits: Iterator[Edit[T]]):
        return self.store.edit_all(edits)
