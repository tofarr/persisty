import itertools
from dataclasses import dataclass
from typing import Optional, Iterator, List

from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.errors import PersistyError
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.page import Page
from persisty.storage.storage_abc import StorageABC, T
from persisty.storage.storage_filter import StorageFilter, append_to_storage_filter
from persisty.storage.wrappers.wrapper_storage_abc import WrapperStorageABC


@dataclass(frozen=True)
class FilteredStorage(WrapperStorageABC[T]):
    """
    Wrapper which covers another and includes a search search_filter. Creates or update not matching the search search_filter
    fail outright. Effectively creates a partial view of another storage (Useful for enforcing security constraints)
    """
    wrapped_storage: StorageABC[T]
    item_filter: ItemFilterABC[T]
    edit_batch_size: int = 100

    @property
    def storage(self) -> StorageABC[T]:
        return self.wrapped_storage

    def create(self, item: T) -> str:
        if not self.item_filter.match(item):
            raise PersistyError(f'invalid_item:{item}')
        return self.storage.create(item)

    def read(self, key: str) -> Optional[T]:
        existing = self.storage.read(key)
        return existing if existing and self.item_filter.match(existing) else None

    def read_all(self, keys: Iterator[str], error_on_missing: bool = True) -> Iterator[T]:
        for item in self.storage.read_all(keys, error_on_missing):
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
        existing = self.storage.read(self.meta.key_config.get_key(item))
        if not existing or not self.item_filter.match(existing):
            raise PersistyError(f'missing_value:{item}')
        return self.storage.update(item)

    def destroy(self, key: str) -> bool:
        existing = self.storage.read(key)
        if not existing or not self.item_filter.match(existing):
            return False  # Silently ignore the change (Just like deleting something that does not exist)
        return self.storage.destroy(key)

    def search(self, storage_filter: Optional[StorageFilter[T]] = None) -> Iterator[T]:
        storage_filter = append_to_storage_filter(storage_filter, self.item_filter)
        items = self.storage.search(storage_filter)
        return items

    def count(self, item_filter: Optional[ItemFilterABC[T]] = None) -> int:
        item_filter = (self.item_filter & item_filter) if item_filter else self.item_filter
        count = self.storage.count(item_filter)
        return count

    def paged_search(self,
                     storage_filter: Optional[StorageFilter[T]] = None,
                     page_key: Optional[str] = None,
                     limit: int = 20
                     ) -> Page[T]:
        storage_filter = append_to_storage_filter(storage_filter, self.item_filter)
        return self.storage.paged_search(storage_filter, page_key, limit)

    def edit_all(self, edits: Iterator[Edit[T]]):
        edits = iter(edits)
        while True:
            batch = list(itertools.islice(edits, self.edit_batch_size))
            if not batch:
                return
            error = self._check_batch(batch)
            if batch:
                self.storage.edit_all(batch)
            if error:
                raise error

    def _check_batch(self, edits: List[Edit[T]]):
        read_keys = (e.key if e.key else self.meta.key_config.get_key(e.item) for e in edits)
        read_keys = (k for k in read_keys if k is not None)
        existing_items = list(self.storage.read_all(read_keys, False))
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
