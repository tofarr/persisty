from abc import abstractmethod
from typing import Optional, List, Tuple

from persisty.errors import PersistyError
from persisty.search_filter.all_items import ALL_ITEMS
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.batch_edit import BatchEditABC, Create, Update, Delete
from persisty.storage.result_set import ResultSet
from persisty.storage.storage_abc import StorageABC, T, F, S
from persisty.storage.storage_meta import StorageMeta
from persisty.util.encrypt_at_rest import decrypt, encrypt


class FilteredStorage(StorageABC[T, F, S]):

    @abstractmethod
    @property
    def storage(self) -> StorageABC[T, F, S]:
        """ Get wrapped storage """

    @property
    def storage_meta(self) -> StorageMeta:
        return self.storage.storage_meta

    def filter_create(self, item: T) -> T:
        """ filter an item before create """

    def filter_update(self, item: T) -> T:
        """ filter an item before create """

    def filter_read(self, item: T) -> Optional[T]:
        """ filter an item after read """

    def filter_filter(self, search_filter: Optional[F]) -> Tuple[Optional[F], bool]:
        """ filter a filter """

    def create(self, item: T) -> T:
        item = self.filter_create(item)
        return self.storage.create(item)

    def read(self, key: str) -> Optional[T]:
        item = self.storage.read(key)
        item = self.filter_read(item)
        return item

    async def read_batch(self, keys: List[str]) -> List[Optional[T]]:
        items = await self.storage.read_batch(keys)
        items = [self.filter_read(item) for item in items]
        return items

    def update(self, item: T) -> Optional[T]:
        key = self.storage_meta.key_config.get_key(item)
        existing_item = self.read(key)
        if not existing_item:
            return None
        item = self.filter_update(item)
        return self.storage.update(item)

    def delete(self, key: str) -> bool:
        # Need to read the item to make sure we have access before deleting it
        return bool(self.read(key)) and self.storage.delete(key)

    def search(self,
               search_filter: Optional[F] = None,
               search_order: Optional[S] = None,
               page_key: Optional[str] = None,
               limit: Optional[int] = None
               ) -> ResultSet[T]:
        if limit is None:
            limit = self.storage_meta.batch_size
        search_filter, fully_handled = self.filter_filter(search_filter)
        if fully_handled:
            return self.storage.search(search_filter, search_order, page_key, limit)
        # Since the nested filter was not fully able to handle the constraint, we handle it here...
        nested_page_key = None
        nested_item_key = None
        if page_key:
            nested_page_key, nested_item_key = decrypt(page_key)
        key_config = self.storage_meta.key_config
        results = []
        while True:
            result_set = self.storage.search(search_filter, search_order, nested_page_key)
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

    def count(self, search_filter: SearchFilterABC = ALL_ITEMS) -> int:
        search_filter, fully_handled = self.filter_filter(search_filter)
        if fully_handled:
            return self.storage.count(search_filter)
        items = self.storage.search_all(search_filter)
        items = (self.filter_read(item) for item in items)
        items = (item for item in items if item)  # Remove filtered items
        count = sum(1 for _ in items)
        return count

    async def edit_batch(self, edits: List[BatchEditABC]):
        keys_to_check = []
        for edit in edits:
            if isinstance(edit, Create):
                edit.item = self.filter_create(edit.item)
            elif isinstance(edit, Update):
                edit.item = self.filter_update(edit.item)
                keys_to_check.append(self.storage)
            elif isinstance(edit, Delete):
                keys_to_check.append(edit.key)
        if keys_to_check:
            items = await self.read_batch(keys_to_check)
            items = [item for item in items if item]
            if len(items) != len(keys_to_check):
                raise PersistyError('unsafe_batch_error')  # It is not safe to proceed with this batch
        return self.storage.edit_batch(edits)
