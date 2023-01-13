from abc import ABC, abstractmethod
from typing import Iterator, List, Optional, Tuple, Dict

from persisty.errors import PersistyError
from persisty.batch_edit import BatchEdit
from persisty.batch_edit_result import BatchEditResult
from persisty.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta
from persisty.store.wrapper_store_abc import WrapperStoreABC, T
from persisty.util import get_logger
from persisty.util.encrypt_at_rest import decrypt, encrypt

logger = get_logger(__name__)


# noinspection PyMethodMayBeStatic
class FilteredStoreABC(WrapperStoreABC[T], ABC):
    """Extension to wrapper storage which loads items before updates / deletes for the purpose of filtering"""

    @abstractmethod
    def get_store(self) -> StoreABC:
        """Get wrapped store"""

    def get_meta(self) -> StoreMeta:
        return self.get_store().get_meta()

    def filter_create(self, item: T) -> Optional[T]:
        """search_filter an stored before create"""
        return item

    # noinspection PyUnusedLocal
    def filter_update(self, item: T, updates: T) -> T:
        """search_filter an stored before create"""
        return updates

    def filter_read(self, item: T) -> Optional[T]:
        """search_filter an stored after read"""
        return item

    # noinspection PyUnusedLocal
    def allow_delete(self, item: T) -> bool:
        """Filter a delete of an stored"""
        return True

    def filter_search_filter(
        self, search_filter: SearchFilterABC
    ) -> Tuple[SearchFilterABC, bool]:
        """
        filter a search_filter. Return the result and a boolean indicating if the filter has been fully
        handled by this operation
        """
        return search_filter, True

    def create(self, item: T) -> Optional[T]:
        item = self.filter_create(item)
        if item:
            return self.get_store().create(item)

    def read(self, key: str) -> Optional[T]:
        item = self.get_store().read(key)
        if item:
            item = self.filter_read(item)
        return item

    def read_batch(self, keys: List[str]) -> List[Optional[T]]:
        assert len(keys) <= self.get_meta().batch_size
        items = self.get_store().read_batch(keys)
        items = [self.filter_read(item) if item else None for item in items]
        return items

    def _update(self, key: str, item: T, updates: T) -> Optional[T]:
        updates = self.filter_update(item, updates)
        if not updates:
            return None
        return self.get_store()._update(key, item, updates)

    def _delete(self, key: str, item: T) -> bool:
        if self.allow_delete(item):
            return self.get_store()._delete(key, item)
        return False

    def search(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[T]:
        if limit is None:
            limit = self.get_meta().batch_size
        else:
            assert limit <= self.get_meta().batch_size
        search_filter, fully_handled = self.filter_search_filter(search_filter)
        if fully_handled:
            return self.get_store().search(search_filter, search_order, page_key, limit)
        # Since the nested search_filter was not fully able to handle the constraint, we handle it here...
        nested_page_key = None
        nested_item_key = None
        if page_key:
            nested_page_key, nested_item_key = decrypt(page_key)
        key_config = self.get_meta().key_config
        results = []
        while True:
            result_set = self.get_store().search(
                search_filter, search_order, nested_page_key
            )
            items = (self.filter_read(item) for item in result_set.results)
            items = (item for item in items if item)  # Remove filtered items

            # Skip over any results we need to in the current page
            while nested_item_key:
                next_item = next(items, None)
                if not next_item:
                    raise PersistyError(
                        "invalid_page_key"
                    )  # The item was probably deleted
                next_item_key = key_config.to_key_str(next_item)
                if next_item_key == nested_item_key:
                    nested_item_key = None

            results.extend(items)
            if len(results) == limit:
                if not result_set.next_page_key:
                    return ResultSet(results)
                return ResultSet(results, encrypt([result_set.next_page_key, None]))
            elif len(results) > limit:
                results = results[:limit]
                return ResultSet(
                    results,
                    encrypt([nested_page_key, key_config.to_key_str(results[-1])]),
                )
            elif not result_set.next_page_key:
                return ResultSet(results)
            else:
                nested_page_key = result_set.next_page_key

    def search_all(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
    ) -> Iterator[T]:
        search_filter, fully_handled = self.filter_search_filter(search_filter)
        items = self.get_store().search_all(search_filter, search_order)
        if fully_handled:
            yield from items
            return
        for item in items:
            item = self.filter_read(item)
            if item:
                yield item

    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        search_filter, fully_handled = self.filter_search_filter(search_filter)
        if fully_handled:
            return self.get_store().count(search_filter)
        items = self.get_store().search_all(search_filter)
        items = (self.filter_read(item) for item in items)
        items = (item for item in items if item)  # Remove filtered items
        count = sum(1 for _ in items)
        return count

    def _edit_batch(
        self, edits: List[BatchEdit[T, T]], items_by_key: Dict[str, T]
    ) -> List[BatchEditResult[T, T]]:
        assert len(edits) <= self.get_meta().batch_size
        key_config = self.get_meta().key_config

        results = [BatchEditResult[T, T](edit, code="unknown") for edit in edits]
        results_by_id = {result.edit.id: result for result in results}
        filtered_edits = []
        for edit in edits:
            result = results_by_id[edit.id]
            try:
                if edit.create_item:
                    item = self.filter_create(edit.create_item)
                    if not item:
                        result.code = "fitered_edit"
                        continue
                    filtered_edits.append(BatchEdit[T, T](create_item=item, id=edit.id))
                elif edit.update_item:
                    key = edit.get_key(key_config)
                    item = items_by_key.get(key)
                    if not item:
                        result.code = "item_missing"
                        continue
                    filtered_updates = self.filter_update(item, edit.update_item)
                    if not filtered_updates:
                        result.code = "filtered_edit"
                        continue
                    filtered_edits.append(
                        BatchEdit[T, T](update_item=filtered_updates, id=edit.id)
                    )
                else:
                    item = items_by_key.get(edit.delete_key)
                    if not item:
                        result.code = "item_missing"
                        continue
                    if not self.allow_delete(item):
                        result.code = "filtered_edit"
                        continue
                    filtered_edits.append(edit)
            except PersistyError as e:
                result.msg = str(e)
        if filtered_edits:
            filtered_results = self.get_store()._edit_batch(
                filtered_edits, items_by_key
            )
            for filtered_result in filtered_results:
                result = results_by_id.get(filtered_result.edit.id)
                # If result is missing, then the nested storage has not implemented the protocol correctly
                if result:
                    result.copy_from(filtered_result)
        return results
