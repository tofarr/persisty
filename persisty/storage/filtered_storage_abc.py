from abc import ABC, abstractmethod
from typing import Iterator, List, Optional, Tuple

from marshy.types import ExternalItemType

from persisty.storage.batch_edit import BatchEditABC, Delete, Update, Create
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta
from persisty.storage.wrapper_storage_abc import WrapperStorageABC
from persisty.util import get_logger
from persisty.util.encrypt_at_rest import decrypt, encrypt

logger = get_logger(__name__)


# noinspection PyMethodMayBeStatic
class FilteredStorageABC(WrapperStorageABC, ABC):
    """ Extension to wrapper storage which loads items before updates / deletes for the purpose of filtering """

    @property
    @abstractmethod
    def storage(self) -> StorageABC:
        """ Get wrapped storage """

    def get_storage_meta(self) -> StorageMeta:
        return self.get_storage().get_storage_meta()

    def filter_create(self, item: ExternalItemType) -> ExternalItemType:
        """ search_filter an stored before create """
        return item

    # noinspection PyUnusedLocal
    def filter_update(self, item: ExternalItemType, updates: ExternalItemType) -> ExternalItemType:
        """ search_filter an stored before create """
        return updates

    def filter_read(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        """ search_filter an stored after read """
        return item

    # noinspection PyUnusedLocal
    def allow_delete(self, item: ExternalItemType) -> bool:
        """ Filter a delete of an stored """
        return True

    def filter_search_filter(self, search_filter: SearchFilterABC) -> Tuple[SearchFilterABC, bool]:
        """
        filter a search_filter. Return the result and a boolean indicating if the filter has been fully
        handled by this operation
        """
        return search_filter, True

    def create(self, item: ExternalItemType) -> ExternalItemType:
        item = self.filter_create(item)
        return self.get_storage().create(item)

    def read(self, key: str) -> Optional[ExternalItemType]:
        item = self.get_storage().read(key)
        item = self.filter_read(item)
        return item

    async def read_batch(self, keys: List[str]) -> List[Optional[ExternalItemType]]:
        assert len(keys) <= self.get_storage_meta().batch_size
        items = await self.get_storage().read_batch(keys)
        items = [self.filter_read(item) for item in items]
        return items

    def update(self,
               updates: ExternalItemType,
               search_filter: SearchFilterABC = INCLUDE_ALL
               ) -> Optional[ExternalItemType]:
        key = self.get_storage_meta().key_config.get_key(updates)
        old_item = self.read(key)
        if not old_item or not search_filter.match(old_item, self.get_storage_meta().fields):
            return None
        item = self.filter_update(old_item, updates)
        if not item:
            return None
        return self.get_storage().update(item)

    def delete(self, key: str) -> bool:
        item = self.get_storage().read(key)
        if item and self.allow_delete(item):
            return self.get_storage().delete(key)
        return False

    def search(self,
               search_filter: SearchFilterABC = INCLUDE_ALL,
               search_order: Optional[SearchOrder] = None,
               page_key: Optional[str] = None,
               limit: Optional[int] = None
               ) -> ResultSet[ExternalItemType]:
        if limit is None:
            limit = self.get_storage_meta().batch_size
        else:
            assert limit <= self.get_storage_meta().batch_size
        search_filter, fully_handled = self.filter_search_filter(search_filter)
        if fully_handled:
            return self.get_storage().search(search_filter, search_order, page_key, limit)
        # Since the nested search_filter was not fully able to handle the constraint, we handle it here...
        nested_page_key = None
        nested_item_key = None
        if page_key:
            nested_page_key, nested_item_key = decrypt(page_key)
        key_config = self.get_storage_meta().key_config
        results = []
        while True:
            result_set = self.get_storage().search(search_filter, search_order, nested_page_key)
            items = (self.filter_read(item) for item in result_set.results)
            items = (item for item in items if item)  # Remove filtered items

            # Skip over any results we need to in the current page
            while nested_item_key:
                next_item = next(items, None)
                if not next_item:
                    return ResultSet([])
                next_item_key = key_config.get_key(next_item)
                if next_item_key == nested_item_key:
                    nested_item_key = None

            results.extend(items)
            if len(results) == limit:
                return ResultSet(results, encrypt([result_set.next_page_key, None]))
            elif len(results) > limit:
                results = results[:limit]
                return ResultSet(results, encrypt([nested_page_key, key_config.get_key(results[-1])]))
            elif not result_set.next_page_key:
                return ResultSet(results)
            else:
                nested_page_key = result_set.next_page_key

    def search_all(self,
                   search_filter: SearchFilterABC = INCLUDE_ALL,
                   search_order: Optional[SearchOrder] = None
                   ) -> Iterator[ExternalItemType]:
        search_filter, fully_handled = self.filter_search_filter(search_filter)
        items = self.get_storage().search_all(search_filter, search_order)
        if fully_handled:
            return items
        for item in items:
            item = self.filter_read(item)
            if item:
                yield item

    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        search_filter, fully_handled = self.filter_search_filter(search_filter)
        if fully_handled:
            return self.get_storage().count(search_filter)
        items = self.get_storage().search_all(search_filter)
        items = (self.filter_read(item) for item in items)
        items = (item for item in items if item)  # Remove filtered items
        count = sum(1 for _ in items)
        return count

    async def edit_batch(self, edits: List[BatchEditABC]):
        assert len(edits) <= self.get_storage_meta().batch_size
        key_config = self.get_storage_meta().key_config

        # Load the items that may be needed for filtering
        keys = list(filter(None, (edit.get_key(key_config) for edit in edits)))
        items = await self.read_batch(keys)
        items_by_key = {key_config.get_key(item): item for item in items if item}

        results = [BatchEditResult(edit, code='unknown') for edit in edits]
        results_by_id = {result.edit.id: result for result in results}
        filtered_edits = []
        for edit in edits:
            if isinstance(edit, Create):
                key = edit.get_key(key_config)
                if key and key in items_by_key:
                    results_by_id[edit.id].code = 'create_existing'
                    continue
                item = self.filter_create(edit.item)
                if not item:
                    results_by_id[edit.id].code = 'fitered_edit'
                    continue
                filtered_edits.append(Create(item, edit.id))
            elif isinstance(edit, Update):
                key = edit.get_key(key_config)
                item = items_by_key.get(key)
                if not item:
                    results_by_id[edit.id].coee = 'item_missing'
                    continue
                filtered_updates = self.filter_update(item, edit.updates)
                if not item:
                    results_by_id[edit.id].code = 'filtered_edit'
                    continue
                filtered_edits.append(Update(filtered_updates, edit.id))
            elif isinstance(edit, Delete):
                key = edit.get_key(key_config)
                item = items_by_key.get(key)
                if not item:
                    results_by_id[edit.id].code = 'item_missing'
                    continue
                if not self.allow_delete(item):
                    results_by_id[edit.id].code = 'filtered_edit'
                    continue
                filtered_edits.append(edit)
            else:
                results_by_id[edit.id].code = 'unsupported_edit_type'
                results_by_id[edit.id].msg = edit.__class__.__name__
        if filtered_edits:
            filtered_results = await self.get_storage().edit_batch(filtered_edits)
            for filtered_result in filtered_results:
                result = results_by_id.get(filtered_result.edit.id)
                if not result:
                    # If result is missing, then the nested storage has not implemented the protocol correctly
                    logger.warning(f'Result missing {filtered_result}')
                    continue
                result.copy_from(filtered_result)
        return results
