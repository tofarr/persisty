from abc import ABC, abstractmethod
from itertools import islice
from typing import Optional, List, Iterator

from marshy.types import ExternalItemType

from persisty.storage.batch_edit import BatchEditABC, Create, Update, Delete
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.result_set import ResultSet
from persisty.storage.search_filter.include_all import INCLUDE_ALL
from persisty.storage.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.search_order import SearchOrder, NO_ORDER
from persisty.storage.storage_meta import StorageMeta


class StorageABC(ABC):
    """
    General contract for storage object, allowing CRUD, search, and batch updates for objects
    """

    @abstractmethod
    @property
    def storage_meta(self) -> StorageMeta:
        """ Get the meta for this storage """

    @abstractmethod
    def create(self, item: ExternalItemType) -> ExternalItemType:
        """ Create an item in the data store """

    @abstractmethod
    def read(self, key: str) -> Optional[ExternalItemType]:
        """ Read an item from the data store """

    async def read_batch(self, keys: List[str]) -> List[Optional[ExternalItemType]]:
        assert(len(keys) <= self.storage_meta.batch_size)
        items = [self.read(key) for key in keys]
        return items

    def read_all(self, keys: Iterator[str]) -> Iterator[Optional[ExternalItemType]]:
        keys = iter(keys)
        while True:
            batch_keys = list(islice(keys, self.storage_meta.batch_size))
            if not batch_keys:
                return
            items = self.read_batch(batch_keys)
            yield from items

    @abstractmethod
    def update(self, updates: ExternalItemType) -> Optional[ExternalItemType]:
        """ Create an item in the data store. By convention any item with a value of UNDEFINED is left alone. """

    @abstractmethod
    def delete(self, key: str) -> bool:
        """ Delete an item from the data store. """

    def search(self,
               search_filter: SearchFilterABC = INCLUDE_ALL,
               search_order: SearchOrder = NO_ORDER,
               page_key: Optional[str] = None,
               limit: Optional[int] = None
               ) -> ResultSet[ExternalItemType]:
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

    def search_all(self,
                   search_filter: SearchFilterABC = INCLUDE_ALL,
                   search_order: SearchOrder = NO_ORDER
                   ) -> Iterator[ExternalItemType]:
        page_key = None
        while True:
            result_set = self.search(search_filter, search_order, page_key)
            yield from result_set.results
            page_key = result_set.next_page_key
            if not page_key:
                return

    @abstractmethod
    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        """ Create an item in the data store """

    async def edit_batch(self, edits: List[BatchEditABC]) -> List[BatchEditResult]:
        """
        Do a batch edit and return a list of results. The results should contain all the same edits in the same
        order
        """
        assert(len(edits) <= self.storage_meta.batch_size)
        results = []
        for edit in edits:
            try:
                if isinstance(edit, Create):
                    item = self.create(edit.item)
                    results.append(BatchEditResult(edit, bool(item)))
                elif isinstance(edit, Update):
                    item = self.update(edit.updates)
                    results.append(BatchEditResult(edit, bool(item)))
                elif isinstance(edit, Delete):
                    deleted = self.delete(edit.key)
                    results.append(BatchEditResult(edit, bool(deleted)))
                else:
                    results.append(BatchEditResult(edit, False, 'unsupported_edit_type', edit.__class__.__name__))
            except Exception as e:
                results.append(BatchEditResult(edit, False, 'exception', str(e)))
        return results

    def edit_all(self, edits: Iterator[BatchEditABC]) -> Iterator[BatchEditResult]:
        edits = iter(edits)
        while True:
            page = list(islice(edits, self.storage_meta.batch_size))
            if not page:
                break
            results = self.edit_batch(page)
            yield from results
