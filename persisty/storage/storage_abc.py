from abc import ABC, abstractmethod
from itertools import islice
from typing import Optional, List, Iterator, Any, Generic, TypeVar

from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order_abc import SearchOrderABC
from persisty.storage.batch_edit import Create, Update, Delete, BatchEditABC
from persisty.storage.result_set import ResultSet
from persisty.storage.storage_meta import StorageMeta

T = TypeVar('T')
F = TypeVar('F', bound=SearchFilterABC)
S = TypeVar('S', bound=SearchOrderABC)


class StorageABC(ABC, Generic[T, F, S]):

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
        """ Create an item in the data store. By convention any item with a value of UNDEFINED is left alone. """

    @abstractmethod
    def delete(self, key: str) -> bool:
        """ Create an item in the data store. By convention any item with a value of UNDEFINED is left alone. """

    def search(self,
               search_filter: Optional[F] = None,
               search_order: Optional[S] = None,
               page_key: Optional[str] = None,
               limit: Optional[int] = None
               ) -> ResultSet[T]:
        if limit is None:
            limit = self.storage_meta.batch_size
        assert(limit <= self.storage_meta.batch_size)
        items = self.search_all(search_filter, search_order)

        if page_key:
            while True:
                next_result = next(items, None)
                if next_result is None:
                    return ResultSet([])
                key = self.storage_meta.key_config.get_key(next_result)
                if key == page_key:
                    break

        items = list(islice(items, limit))

        page_key = None
        if len(items) == limit:
            page_key = self.storage_meta.key_config.get_key(items[-1])

        return ResultSet(items, page_key)

    def search_all(self, search_filter: Optional[F] = None, search_order: Optional[S] = None) -> Iterator[T]:
        page_key = None
        while True:
            result_set = self.search(search_filter, search_order, page_key)
            yield from result_set.results
            page_key = result_set.next_page_key
            if not page_key:
                return

    @abstractmethod
    def count(self, search_filter: Optional[F] = None) -> int:
        """ Create an item in the data store """

    async def edit_batch(self, edits: List[BatchEditABC]):
        assert(len(edits) <= self.storage_meta.batch_size)
        for edit in edits:
            if isinstance(edit, Create):
                self.create(edit.item)
            elif isinstance(edit, Update):
                self.update(edit.item)
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
