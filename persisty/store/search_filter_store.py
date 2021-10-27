import itertools
from dataclasses import dataclass
from typing import Callable, Optional, Iterator, Any, List

from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.errors import PersistyError, ValidationError
from persisty.page import Page
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC, T
from persisty.util import from_base64, to_base64


@dataclass(frozen=True)
class SearchFilterStore(WrapperStoreABC[T]):
    """
    Wrapper which covers another and includes a search filter. Creates or update not matching the search filter
    fail outright. Effectively creates a partial view of another store (Useful for enforcing security constraints)
    """
    store: StoreABC[T]
    item_filter: Callable[[T], bool]
    search_filter_factory: Callable[[Any], Any] = None
    edit_batch_size: int = 100

    def create(self, item: T) -> str:
        if not self.item_filter(item):
            raise ValidationError(f'invalid_item:{item}')
        return self.store.create(item)

    def read(self, key: str) -> Optional[T]:
        existing = self.store.read(key)
        return self.item_filter(existing) if existing else None

    def read_all(self, keys: Iterator[str], error_on_missing: bool = True) -> Iterator[T]:
        for item in self.store.read_all(keys, error_on_missing):
            if self.item_filter(item):
                yield item
            elif error_on_missing:
                raise PersistyError(f'missing_item')
            else:
                yield None

    def update(self, item: T) -> T:
        if not self.item_filter(item):
            raise ValidationError(f'invalid_item:{item}')
        existing = self.store.read(self.store.get_key(item))
        if not existing or not self.item_filter(existing):
            raise PersistyError(f'missing_value:{item}')
        return self.store.update(item)

    def destroy(self, key: str) -> bool:
        existing = self.store.read(self.store.get_key(key))
        if not self.item_filter(existing):
            return False  # Silently ignore the change (Just like deleting something that does not exist)
        return self.store.destroy(key)

    def search(self, search_filter: Any = None) -> Iterator[T]:
        if self.search_filter_factory:
            search_filter = self.search_filter_factory(search_filter)
            return self.store.search(search_filter)
        items = self.store.search(search_filter)
        for item in items:
            if self.item_filter(item):
                yield item

    def count(self, search_filter: Any = None) -> int:
        if self.search_filter_factory:
            search_filter = self.search_filter_factory(search_filter)
            return self.store.count(search_filter)
        items = self.store.search(search_filter)
        count = sum(1 for item in items if self.item_filter(item))
        return count

    def paged_search(self, search_filter: Any = None, page_key: str = None, limit: int = 20) -> Page[T]:
        if self.search_filter_factory:
            search_filter = self.search_filter_factory(search_filter)
            return self.store.paged_search(search_filter, page_key, limit)
        item_key = None
        if page_key:
            page_key, item_key = from_base64(page_key)

        page = self.store.paged_search(search_filter, page_key, limit)
        page_items = iter(page.items)
        # Skip items that were already processed
        if item_key:
            while True:
                item = next(page_items)
                if self.get_key(item) == item_key:
                    break
        items = []
        while True:
            for item in page_items:
                if self.item_filter(item):
                    items.append(item)
                    if len(items) == limit:
                        next_page_key = to_base64([page_key, self.get_key(item)])
                        return Page(items, next_page_key)
            if page.next_page_key:
                page_key = page.next_page_key
                page = self.store.paged_search(search_filter, page_key, limit)
                page_items = iter(page.items)
            else:
                return Page(items)

    def edit_all(self, edits: Iterator[Edit[T]]):
        while True:
            batch = list(itertools.islice(edits, self.edit_batch_size))
            if not batch:
                return
            self._check_batch(batch)
            self.store.edit_all(batch)

    def _check_batch(self, edits: List[Edit[T]]):
        # Validate incoming
        for edit in edits:
            if edit.edit_type in (EditType.CREATE, EditType.UPDATE):
                if not self.item_filter(edit.value):
                    raise ValidationError(f'invalid_item:{edit.value}:update')

        # Validate existing
        read_keys = (e.key if e.key else self.get_key(e.value)
                     for e in edits if e.edit_type in [EditType.CREATE, EditType.UPDATE])
        list(self.read_all(read_keys, True))
