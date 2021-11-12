import itertools
from dataclasses import dataclass
from typing import Optional, Iterator, List

from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.errors import PersistyError
from persisty2.item_filter.item_filter_abc import ItemFilterABC
from persisty.page import Page
from persisty2.search_filter import SearchFilter, append_to_search_filter
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC, T


@dataclass(frozen=True)
class SearchFilterStore(WrapperStoreABC[T]):
    """
    Wrapper which covers another and includes a search filter. Creates or update not matching the search filter
    fail outright. Effectively creates a partial view of another store (Useful for enforcing security constraints)
    """
    wrapped_store: StoreABC[T]
    item_filter: ItemFilterABC[T]
    edit_batch_size: int = 100

    @property
    def store(self) -> StoreABC[T]:
        return self.wrapped_store

    def create(self, item: T) -> str:
        if not self.item_filter.match(item):
            raise PersistyError(f'invalid_item:{item}')
        return self.store.create(item)

    def read(self, key: str) -> Optional[T]:
        existing = self.store.read(key)
        return existing if existing and self.item_filter.match(existing) else None

    def read_all(self, keys: Iterator[str], error_on_missing: bool = True) -> Iterator[T]:
        for item in self.store.read_all(keys, error_on_missing):
            if item is None:
                yield None
            elif self.item_filter.match(item):
                yield item
            elif error_on_missing:
                raise PersistyError(f'missing_item')
            else:
                yield None

    def update(self, item: T) -> T:
        if not self.item_filter.match(item):
            raise PersistyError(f'invalid_item:{item}')
        existing = self.store.read(self.store.get_key(item))
        if not existing or not self.item_filter.match(existing):
            raise PersistyError(f'missing_value:{item}')
        return self.store.update(item)

    def destroy(self, key: str) -> bool:
        existing = self.store.read(key)
        if not existing or not self.item_filter.match(existing):
            return False  # Silently ignore the change (Just like deleting something that does not exist)
        return self.store.destroy(key)

    def search(self, search_filter: Optional[SearchFilter[T]] = None) -> Iterator[T]:
        search_filter = append_to_search_filter(search_filter, self.item_filter)
        items = self.store.search(search_filter)
        return items

    def count(self, item_filter: Optional[ItemFilterABC[T]] = None) -> int:
        item_filter = (self.item_filter & item_filter) if item_filter else self.item_filter
        count = self.store.count(item_filter)
        return count

    def paged_search(self,
                     search_filter: Optional[SearchFilter[T]] = None,
                     page_key: Optional[str] = None,
                     limit: int = 20
                     ) -> Page[T]:
        search_filter = append_to_search_filter(search_filter, self.item_filter)
        return self.store.paged_search(search_filter, page_key, limit)

    def edit_all(self, edits: Iterator[Edit[T]]):
        edits = iter(edits)
        while True:
            batch = list(itertools.islice(edits, self.edit_batch_size))
            if not batch:
                return
            error = self._check_batch(batch)
            if batch:
                self.store.edit_all(batch)
            if error:
                raise error

    def _check_batch(self, edits: List[Edit[T]]):
        read_keys = (e.key if e.key else self.get_key(e.item) for e in edits)
        read_keys = (k for k in read_keys if k is not None)
        existing_items = list(self.store.read_all(read_keys, False))
        for index, item in enumerate(list(existing_items)):
            if item is None:
                continue
            edit = edits[index]
            if edit.edit_type in (EditType.CREATE, EditType.UPDATE):
                if not self.item_filter.match(edit.item):
                    del edits[index:]
                    return PersistyError(f'invalid_key:{read_keys}')
            elif not self.item_filter.match(item):
                if edit.edit_type == EditType.DESTROY:
                    del existing_items[index]
                    del edits[index]
