from abc import ABC, abstractmethod
from itertools import islice
from typing import Optional, List, Iterator, Any, Generic, TypeVar

from persisty.batch_edit import BatchEditABC, Create, Update, Delete
from persisty.result_set import ResultSet
from persisty.search_filter.all_items import ALL_ITEMS
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order_abc import SearchOrderABC
from persisty.storage.storage_meta import StorageMeta

T = TypeVar('T')
F = TypeVar('F', bound=SearchFilterABC)
C = TypeVar('C', bound=SearchOrderABC)


class StorageABC(ABC, Generic[T, F, C]):

    @abstractmethod
    @property
    def storage_meta(self) -> StorageMeta:
        """ Get the meta for this storage """

    @abstractmethod
    def create(self, item: T) -> T:
        """ Create an item in the data store """

    @abstractmethod
    def read(self, key: str) -> Optional[T]:
        """ Create an item in the data store """

    async def read_batch(self, keys: List[str]) -> List[Optional[T]]:
        assert(len(keys) <= self.storage_meta.batch_size)
        items = [self.read(key) for key in keys]
        return items

    def read_all(self, keys: Iterator[str]) -> Iterator[Optional[T]]:
        keys = iter(keys)
        while True:
            batch_keys = list(islice(keys, self.storage_meta.batch_size))
            if not batch_keys:
                return
            items = self.read_batch(batch_keys)
            yield from items

    @abstractmethod
    def update(self, item: T) -> Optional[T]:
        """ Create an item in the data store. By convention any field with a value of UNDEFINED is left alone. """

    @abstractmethod
    def delete(self, key: str) -> bool:
        """ Create an item in the data store. By convention any field with a value of UNDEFINED is left alone. """

    @abstractmethod
    def search(self,
               search_filter: SearchFilterABC = ALL_ITEMS,
               search_order: Optional[SearchOrderABC] = None,
               page_key: Optional[str] = None,
               limit: Optional[int] = None
               ) -> ResultSet[T]:
        """ Create an item in the data store. """

    def search_all(self,
                   search_filter: SearchFilterABC = ALL_ITEMS,
                   search_order: Optional[SearchOrderABC] = None
                   ) -> Iterator[Any]:
        page_key = None
        while True:
            result_set = self.search(search_filter, search_order, page_key)
            yield from result_set.results
            page_key = result_set.next_page_key
            if not page_key:
                return

    @abstractmethod
    def count(self, search_filter: SearchFilterABC = ALL_ITEMS) -> int:
        """ Create an item in the data store """

    async def edit_batch(self, edits: List[BatchEditABC]):
        assert(len(edits) <= self.storage_meta.batch_size)
        for edit in edits:
            if isinstance(edit, Create):
                self.create(edit.entity)
            elif isinstance(edit, Update):
                self.update(edit.entity)
            elif isinstance(edit, Delete):
                self.delete(edit.key)
            else:
                raise TypeError(f'unknown_type:{edit}')

    def edit_all(self, edits: Iterator[BatchEditABC]):
        edits = iter(edits)
        while True:
            page = list(islice(edits, self.storage_meta.batch_size))
            if not page:
                break
            self.edit_batch(page)
