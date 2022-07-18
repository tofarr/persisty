from abc import abstractmethod
from itertools import islice
from typing import Optional, List, Iterator, Generic

from marshy.types import ExternalItemType

from persisty.obj_storage.obj_storage_meta import ObjStorageMeta, T, F, S, C, U
from persisty.storage.batch_edit import BatchEditABC, Create, Update, Delete
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.result_set import ResultSet
from persisty.storage.storage_abc import skip_to_page


class ObjStorageABC(Generic[T, F, S, C, U]):

    @abstractmethod
    @property
    def obj_storage_meta(self) -> ObjStorageMeta[T, F, S, C, U]:
        """ Get the type for items returned """

    @abstractmethod
    def create(self, item: C) -> T:
        """ Create an stored """

    @abstractmethod
    def read(self, key: str) -> Optional[ExternalItemType]:
        """ Read an stored from the data store """

    async def read_batch(self, keys: List[str]) -> List[Optional[T]]:
        items = [self.read(key) for key in keys]
        return items

    def read_all(self, keys: Iterator[str]) -> Iterator[Optional[T]]:
        keys = iter(keys)
        while True:
            batch_keys = list(islice(keys, self.obj_storage_meta.batch_size))
            if not batch_keys:
                return
            items = self.read_batch(batch_keys)
            yield from items

    @abstractmethod
    def update(self, updates: U) -> Optional[T]:
        """ Create an stored in the data store """

    @abstractmethod
    def delete(self, key: str) -> bool:
        """ Delete an stored from the data store. """

    def search(self,
               search_filter_factory: Optional[F] = None,
               search_order_factory: Optional[S] = None,
               page_key: Optional[str] = None,
               limit: Optional[int] = None
               ) -> ResultSet[T]:
        if limit is None:
            limit = self.obj_storage_meta.batch_size
        assert(limit <= self.obj_storage_meta.batch_size)
        items = self.search_all(search_filter_factory, search_order_factory)
        skip_to_page(page_key, items, self.obj_storage_meta.key_config)
        items = list(islice(items, limit))
        page_key = None
        if len(items) == limit:
            page_key = self.obj_storage_meta.key_config.get_key(items[-1])
        return ResultSet(items, page_key)

    def search_all(self,
                   search_filter_factory: Optional[F] = None,
                   search_order_factory: Optional[S] = None
                   ) -> Iterator[ExternalItemType]:
        page_key = None
        while True:
            result_set = self.search(search_filter_factory, search_order_factory, page_key)
            yield from result_set.results
            page_key = result_set.next_page_key
            if not page_key:
                return

    @abstractmethod
    def count(self, search_filter_factory: Optional[F] = None) -> int:
        """ Get a count of all matching items """

    async def edit_batch(self, edits: List[BatchEditABC]) -> List[BatchEditResult]:
        assert(len(edits) <= self.obj_storage_meta.batch_size)
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

    def edit_all(self, edits: Iterator[BatchEditABC]):
        edits = iter(edits)
        while True:
            page = list(islice(edits, self.obj_storage_meta.batch_size))
            if not page:
                break
            results = self.edit_batch(page)
            yield from results
