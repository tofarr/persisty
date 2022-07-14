import logging
from dataclasses import field, dataclass
from typing import Optional, Iterator

from persisty.edit import Edit
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.page import Page

from persisty.util import get_logger
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_filter import StorageFilter
from persisty.storage.wrappers.wrapper_storage_abc import WrapperStorageABC, T


@dataclass(frozen=True)
class LoggingStorage(WrapperStorageABC[T]):
    """ Storage which logs everything going in and out. Useful for debugging. """
    wrapped_storage: StorageABC[T]
    logger: logging.Logger = field(default_factory=lambda: get_logger(__name__))

    @property
    def storage(self):
        return self.wrapped_storage

    def create(self, item: T) -> str:
        self.logger.info(f'{self.meta.name}:before_create:{item}')
        key = self.storage.create(item)
        self.logger.info(f'{self.meta.name}:after_create:{key}:{item}')
        return key

    def read(self, key: str) -> Optional[T]:
        item = self.storage.read(key)
        self.logger.info(f'{self.meta.name}:read:{key}:{item}')
        return item

    def read_all(self, keys: Iterator[str], error_on_missing: bool = True) -> Iterator[T]:
        for item in self.storage.read_all(keys, error_on_missing):
            self.logger.info(f'{self.meta.name}:read_all:{item}')
            yield item

    def update(self, item: T) -> T:
        self.logger.info(f'{self.meta.name}:before_update:{item}')
        item = self.storage.update(item)
        self.logger.info(f'{self.meta.name}:after_update:{item}')
        return item

    def destroy(self, key: str) -> bool:
        destroyed = self.storage.destroy(key)
        self.logger.info(f'{self.meta.name}:after_destroy:{key}:{destroyed}')
        return destroyed

    def search(self, storage_filter: Optional[StorageFilter[T]] = None) -> Iterator[T]:
        self.logger.info(f'{self.meta.name}:before_search:{storage_filter}')
        items = self.storage.search(storage_filter)
        for item in items:
            self.logger.info(f'{self.meta.name}:search_result:{item}')
            yield item

    def count(self, item_filter: Optional[ItemFilterABC[T]] = None) -> int:
        count = self.storage.count(item_filter)
        self.logger.info(f'{self.meta.name}:count:{item_filter}:{count}')
        return count

    def paged_search(self,
                     storage_filter: Optional[StorageFilter[T]] = None,
                     page_key: str = None,
                     limit: int = 20
                     ) -> Page[T]:
        page = self.storage.paged_search(storage_filter, page_key, limit)
        self.logger.info(f'{self.meta.name}:paged_search:{storage_filter}:{page_key}:{limit}:{page}')
        return page

    def edit_all(self, edits: Iterator[Edit[T]]):
        edits = self._filter_edits(edits)
        self.storage.edit_all(edits)

    def _filter_edits(self, edits: Iterator[Edit[T]]) -> Iterator[Edit[T]]:
        for edit in edits:
            self.logger.info(f'{self.meta.name}:batch_edit:{edit}')
            yield edit
