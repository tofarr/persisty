from abc import ABC, abstractmethod
from itertools import islice
from typing import Optional, List, Iterator, Dict

from marshy.types import ExternalItemType

from persisty.errors import PersistyError
from persisty.storage.batch_edit import BatchEdit
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

    def update(
        self, updates: ExternalItemType, search_filter: SearchFilterABC = INCLUDE_ALL
    ) -> Optional[ExternalItemType]:
        """
        Update (a partial set of values from) an item based upon its key and the constraint given. By convention
        any UNDEFINED value is ignored. Return the full new version of the item if an update occured. If the key
        extracted from the updates did not match any existing item, return None. If any other error occurrecd, throw
        a PersistyError
        """
        key = self.get_storage_meta().key_config.to_key_str(updates)
        if not key:
            raise PersistyError(f"missing_key:{updates}")
        item = self.read(key)
        if item and search_filter.match(item, self.get_storage_meta().fields):
            return self._update(key, item, updates, search_filter)

    @abstractmethod
    def _update(
        self,
        key: str,
        item: ExternalItemType,
        updates: ExternalItemType,
        search_filter: SearchFilterABC = INCLUDE_ALL,
    ) -> Optional[ExternalItemType]:
        """
        Update (a partial set of values from) an item based upon its key and the constraint given. By convention
        any UNDEFINED value is ignored. Return the full new version of the item if an update occured. If the key
        extracted from the updates did not match any existing item, return None. If any other error occurrecd, throw
        a PersistyError
        """

    def delete(self, key: str) -> bool:
        """Delete an stored from the data store. Return true if an item was deleted, false otherwise"""
        item = self.read(key)
        if not item:
            return False
        return self._delete(key, item)

    @abstractmethod
    def _delete(self, key: str, item: ExternalItemType) -> bool:
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

    def edit_batch(self, edits: List[BatchEdit]) -> List[BatchEditResult]:
        """
        Do a batch edit and return a list of results. The results should contain all the same edits in the same
        order
        """
        assert len(edits) <= self.get_storage_meta().batch_size
        to_key_str = self.get_storage_meta().key_config.to_key_str
        keys = []
        for edit in edits:
            if edit.create_item:
                key = to_key_str(edit.create_item)
                if key:
                    keys.append(key)
            if edit.update_item:
                keys.append(to_key_str(edit.update_item))
            elif edit.delete_key:
                keys.append(edit.delete_key)
        items_by_key = {
            to_key_str(item): item for item in self.read_batch(keys) if item
        }
        filtered_edits = []
        for edit in edits:
            if edit.create_item:
                key = to_key_str(edit.create_item)
                if key:
                    item = items_by_key.get(key)
                    if item:
                        continue
                filtered_edits.append(edit)
                continue
            if edit.update_item:
                key = to_key_str(edit.update_item)
                if key in items_by_key:
                    filtered_edits.append(edit)
            elif edit.delete_key and edit.delete_key in items_by_key:
                filtered_edits.append(edit)
        filtered_results = self._edit_batch(filtered_edits, items_by_key)
        filtered_results_by_id = {r.edit.id: r for r in filtered_results}
        results = [
            filtered_results_by_id.get(e.id)
            or BatchEditResult(
                e, False, "duplicate_key" if e.create_item else "missing_key"
            )
            for e in edits
        ]
        return results

    def _edit_batch(
        self, edits: List[BatchEdit], items_by_key: Dict[str, ExternalItemType]
    ) -> List[BatchEditResult]:
        """
        Simple non transactional implementation of batch functionality. Other implementations employ strategies to boost
        performance such reducing the number of network round trips. Whether an edit is atomic is dependant on the
        underlying mechanism, but should be reflected in the results.
        """
        results = []
        to_key_str = self.get_storage_meta().key_config.to_key_str
        for edit in edits:
            try:
                if edit.create_item:
                    item = self.create(edit.create_item)
                    results.append(BatchEditResult(edit, bool(item)))
                elif edit.update_item:
                    key = to_key_str(edit.update_item)
                    item = items_by_key[key]
                    item = self._update(key, item, edit.update_item)
                    results.append(BatchEditResult(edit, bool(item)))
                else:
                    item = items_by_key.get(edit.delete_key)
                    deleted = self._delete(edit.delete_key, item)
                    results.append(BatchEditResult(edit, bool(deleted)))
            except Exception as e:
                results.append(BatchEditResult(edit, False, "exception", str(e)))
        return results

    def edit_all(self, edits: Iterator[BatchEdit]) -> Iterator[BatchEditResult]:
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
