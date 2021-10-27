from abc import ABC
from dataclasses import dataclass
from typing import Optional, Iterator, Type, Any, TypeVar

from marshy.marshaller.marshaller_abc import MarshallerABC

from persisty.capabilities import Capabilities
from persisty.edit import Edit
from persisty.page import Page
from persisty.store.store_abc import StoreABC, T

U = TypeVar('U')


@dataclass(frozen=True)
class TransformingStore(StoreABC[T, U], ABC):
    """ Store which takes one type of object and transform. Primarily used for composition """
    store: StoreABC[U]
    outer_marshaller: MarshallerABC[T]
    inner_marshaller: MarshallerABC[U]
    outer_search_filter_marshaller: MarshallerABC
    inner_search_filter_marshaller: MarshallerABC
    name: str = None

    @property
    def item_type(self) -> Type[T]:
        return self.outer_marshaller.marshalled_type

    @property
    def capabilities(self) -> Capabilities:
        return self.store.capabilities

    def _transform_input(self, item: T) -> U:
        dumped = self.outer_marshaller.dump(item)
        loaded = self.inner_marshaller.load(dumped)
        return loaded

    def _transform_output(self, item: U) -> T:
        dumped = self.inner_marshaller.dump(item)
        loaded = self.outer_marshaller.load(dumped)
        return loaded

    def get_key(self, item: T) -> str:
        return self.store.get_key(self._transform_input(item))

    def create(self, item: T) -> str:
        return self.store.create(self._transform_input(item))

    def read(self, key: str) -> Optional[T]:
        read = self.store.read(key)
        return None if read is None else self._transform_output(read)

    def read_all(self, keys: Iterator[str], error_on_missing: bool = True) -> Iterator[T]:
        for item in self.store.read_all(keys, error_on_missing):
            yield self._transform_output(item)

    def update(self, item: T) -> T:
        return self._transform_output(self.store.update(self._transform_input(item)))

    def destroy(self, key: str) -> bool:
        return self.store.destroy(key)

    def _transform_search_filter(self, search_filter: Any):
        dumped = self.outer_search_filter_marshaller.dump(search_filter)
        loaded = self.inner_search_filter_marshaller.load(dumped)
        return loaded

    def search(self, search_filter: Any = None) -> Iterator[T]:
        for item in self.store.search(self._transform_search_filter(search_filter)):
            yield self._transform_output(item)

    def count(self, search_filter: Any = None) -> int:
        return self.store.count(self._transform_search_filter(search_filter))

    def paged_search(self, search_filter: Any = None, page_key: str = None, limit: int = 20) -> Page[T]:
        page = self.store.paged_search(self._transform_search_filter(search_filter), page_key, limit)
        items = [self._transform_output(item) for item in page.items]
        return Page(items, page.next_page_key)

    def edit_all(self, edits: Iterator[Edit[T]]):
        edits = (self._transform_edit(edit) for edit in edits)
        return self.store.edit_all(edits)

    def _transform_edit(self, edit: Edit[T]) -> Edit[U]:
        value = None if edit.value is None else self._transform_input(edit.value)
        return Edit(edit.edit_type, edit.key, value)
