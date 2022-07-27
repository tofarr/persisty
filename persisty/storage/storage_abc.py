from abc import ABC, abstractmethod
from itertools import islice
from typing import Optional, List, Iterator

from marshy.types import ExternalItemType

from persisty.errors import PersistyError
from persisty.storage.batch_edit import BatchEditABC, Create, Update, Delete
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.storage.storage_meta import StorageMeta


class StorageABC(ABC):
    """
    General contract for storage object, allowing CRUD, search, and batch updates for objects
    """

    @abstractmethod
    def get_storage_meta(self) -> StorageMeta:
        """Get the meta for this storage"""

    @abstractmethod
    def create(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        """Create an stored in the data store"""

    @abstractmethod
    def read(self, key: str) -> Optional[ExternalItemType]:
        """Read an stored from the data store"""

    def read_batch(self, keys: List[str]) -> List[Optional[ExternalItemType]]:
        assert len(keys) <= self.get_storage_meta().batch_size
        items = [self.read(key) for key in keys]
        return items

    def read_all(self, keys: Iterator[str]) -> Iterator[Optional[ExternalItemType]]:
        keys = iter(keys)
        while True:
            batch_keys = list(islice(keys, self.get_storage_meta().batch_size))
            if not batch_keys:
                return
            items = self.read_batch(batch_keys)
            yield from items

    @abstractmethod
    def update(
        self, updates: ExternalItemType, search_filter: SearchFilterABC = INCLUDE_ALL
    ) -> Optional[ExternalItemType]:
        """
        Update (a partial set of values from) an item based upon its key and the constraint given. By convention
        any UNDEFINED value is ignored. Return the full new version of the item if an update occured. If the key
        extracted from the updates did not match any existing item, return None. If any other error occurrecd, throw
        a PersistyError
        """

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete an stored from the data store. Return true if an item was deleted, false otherwise"""

    def search(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[ExternalItemType]:
        if limit is None:
            limit = self.get_storage_meta().batch_size
        assert limit <= self.get_storage_meta().batch_size
        items = self.search_all(search_filter, search_order)
        skip_to_page(page_key, items, self.get_storage_meta().key_config)
        items = list(islice(items, limit))
        page_key = None
        if len(items) == limit:
            page_key = self.get_storage_meta().key_config.to_key_str(items[-1])
        return ResultSet(items, page_key)

    def search_all(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
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
        """Create an stored in the data store"""

    def edit_batch(self, edits: List[BatchEditABC]) -> List[BatchEditResult]:
        """
        Do a batch edit and return a list of results. The results should contain all the same edits in the same
        order
        """
        assert len(edits) <= self.get_storage_meta().batch_size
        return edit_batch(self, edits)

    def edit_all(self, edits: Iterator[BatchEditABC]) -> Iterator[BatchEditResult]:
        edits = iter(edits)
        while True:
            page = list(islice(edits, self.get_storage_meta().batch_size))
            if not page:
                break
            results = self.edit_batch(page)
            yield from results


def skip_to_page(page_key: str, items, key_config):
    if page_key:
        while True:
            next_result = next(items, None)
            if next_result is None:
                raise PersistyError("invalid_page_key")  # The item was probably deleted
            key = key_config.to_key_str(next_result)
            if key == page_key:
                return


def edit_batch(storage, edits: List[BatchEditABC]) -> List[BatchEditResult]:
    """
    Simple non transactional implementation of batch functionality. Other implementations employ strategies to boost
    performance such reducing the number of network round trips.
    """
    results = []
    for edit in edits:
        try:
            if isinstance(edit, Create):
                item = storage.create(edit.item)
                results.append(BatchEditResult(edit, bool(item)))
            elif isinstance(edit, Update):
                item = storage.update(edit.updates)
                results.append(BatchEditResult(edit, bool(item)))
            elif isinstance(edit, Delete):
                deleted = storage.delete(edit.key)
                results.append(BatchEditResult(edit, bool(deleted)))
            else:
                results.append(
                    BatchEditResult(
                        edit, False, "unsupported_edit_type", edit.__class__.__name__
                    )
                )
        except Exception as e:
            results.append(BatchEditResult(edit, False, "exception", str(e)))
    return results


def search(storage, storage_meta, search_filter, search_order, page_key, limit):
    if limit is None:
        limit = storage_meta.batch_size
    assert limit <= storage_meta.batch_size
    items = storage.search_all(search_filter, search_order)
    skip_to_page(page_key, items, storage_meta.key_config)
    items = list(islice(items, limit))
    page_key = None
    if len(items) == limit:
        page_key = storage_meta.key_config.to_key_str(items[-1])
    return ResultSet(items, page_key)
