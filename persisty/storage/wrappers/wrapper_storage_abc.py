from abc import abstractmethod
from typing import Optional, Iterator

from persisty.edit import Edit
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.page import Page
from persisty.storage.storage_abc import StorageABC, T
from persisty.storage.storage_filter import StorageFilter
from persisty.storage.storage_meta import StorageMeta


class WrapperStorageABC(StorageABC[T]):

    @abstractmethod
    @property
    def storage(self) -> StorageABC[T]:
        """ Get the wrapped storage """

    @property
    def item_type(self):
        return self.storage.item_type

    @property
    def meta(self) -> StorageMeta:
        return self.storage.meta

    def create(self, item: T) -> str:
        return self.storage.create(item)

    def read(self, key: str) -> Optional[T]:
        return self.storage.read(key)

    def read_all(self, keys: Iterator[str], error_on_missing: bool = True) -> Iterator[T]:
        return self.storage.read_all(keys, error_on_missing)

    def update(self, item: T) -> T:
        return self.storage.update(item)

    def destroy(self, key: str) -> bool:
        return self.storage.destroy(key)

    def search(self, storage_filter: Optional[StorageFilter[T]] = None) -> Iterator[T]:
        return self.storage.search(storage_filter)

    def count(self, item_filter: Optional[ItemFilterABC[T]] = None) -> int:
        return self.storage.count(item_filter)

    def paged_search(self,
                     storage_filter: Optional[StorageFilter[T]] = None,
                     page_key: str = None,
                     limit: int = 20
                     ) -> Page[T]:
        return self.storage.paged_search(storage_filter, page_key, limit)

    def edit_all(self, edits: Iterator[Edit[T]]):
        return self.storage.edit_all(edits)
