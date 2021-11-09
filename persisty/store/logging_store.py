import logging
from dataclasses import field, dataclass
from typing import Optional, Iterator

from persisty.edit import Edit
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.page import Page
from persisty.search_filter import SearchFilter
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC, T
from persisty.util import get_logger


@dataclass(frozen=True)
class LoggingStore(WrapperStoreABC[T]):
    """ Store which logs everything going in and out. Useful for debugging. """
    wrapped_store: StoreABC[T]
    logger: logging.Logger = field(default_factory=lambda: get_logger(__name__))

    @property
    def store(self):
        return self.wrapped_store

    def create(self, item: T) -> str:
        self.logger.info(f'{self.name}:before_create:{item}')
        key = self.store.create(item)
        self.logger.info(f'{self.name}:after_create:{key}:{item}')
        return key

    def read(self, key: str) -> Optional[T]:
        item = self.store.read(key)
        self.logger.info(f'{self.name}:read:{key}:{item}')
        return item

    def read_all(self, keys: Iterator[str], error_on_missing: bool = True) -> Iterator[T]:
        for item in self.store.read_all(keys, error_on_missing):
            self.logger.info(f'{self.name}:read_all:{item}')
            yield item

    def update(self, item: T) -> T:
        self.logger.info(f'{self.name}:before_update:{item}')
        item = self.store.update(item)
        self.logger.info(f'{self.name}:after_update:{item}')
        return item

    def destroy(self, key: str) -> bool:
        destroyed = self.store.destroy(key)
        self.logger.info(f'{self.name}:after_destroy:{key}:{destroyed}')
        return destroyed

    def search(self, search_filter: Optional[SearchFilter[T]] = None) -> Iterator[T]:
        self.logger.info(f'{self.name}:before_search:{search_filter}')
        items = self.store.search(search_filter)
        for item in items:
            self.logger.info(f'{self.name}:search_result:{item}')
            yield item

    def count(self, item_filter: Optional[ItemFilterABC[T]] = None) -> int:
        count = self.store.count(item_filter)
        self.logger.info(f'{self.name}:count:{item_filter}:{count}')
        return count

    def paged_search(self,
                     search_filter: Optional[SearchFilter[T]] = None,
                     page_key: str = None,
                     limit: int = 20
                     ) -> Page[T]:
        page = self.store.paged_search(search_filter, page_key, limit)
        self.logger.info(f'{self.name}:paged_search:{search_filter}:{page_key}:{limit}:{page}')
        return page

    def edit_all(self, edits: Iterator[Edit[T]]):
        edits = self._filter_edits(edits)
        self.store.edit_all(edits)

    def _filter_edits(self, edits: Iterator[Edit[T]]) -> Iterator[Edit[T]]:
        for edit in edits:
            self.logger.info(f'{self.name}:batch_edit:{edit}')
            yield edit
