from abc import ABC
from dataclasses import dataclass, field
from typing import Optional, Iterator, Type, Any

from persisty import PersistyContext, get_persisty_context
from persisty.capabilities import Capabilities
from persisty.edit import Edit
from persisty.page import Page
from persisty.store.store_abc import StoreABC, T
from persisty.store.wrapper_store_abc import WrapperStoreABC


@dataclass(frozen=True)
class DeferredStore(WrapperStoreABC[T], ABC):
    """ Store which lazily grabs another store from the persisty context. Primarily used for composite stores. """
    name: str
    persisty_context: PersistyContext = field(default_factory=get_persisty_context)

    @property
    def store(self) -> StoreABC[T]:
        store = self.persisty_context.get_store(self.name)
        return store

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
