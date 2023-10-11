from dataclasses import dataclass
from typing import Optional, List, Dict

from servey.security.authorization import Authorization

from persisty.batch_edit import BatchEdit
from persisty.batch_edit_result import BatchEditResult
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC, T


@dataclass
class LinkedStore(WrapperStoreABC[T]):
    """
    Store which enforces links between items and stores
    """

    store: StoreABC[T]
    authorization: Authorization

    def get_store(self) -> StoreABC:
        return self.store

    def _delete(self, key: str, item: T) -> bool:
        for link in self.get_meta().links:
            link.before_delete(item)
        # pylint: disable=W0212
        result = self.get_store()._delete(key, item)
        if result:
            for link in self.get_meta().links:
                link.after_delete(item)
        return result

    def _update(self, key: str, item: T, updates: T) -> Optional[T]:
        for link in self.get_meta().links:
            link.before_update(item, updates)
        # pylint: disable=W0212
        result = self.get_store()._update(key, item, updates)
        for link in self.get_meta().links:
            link.after_update(item, result)
        return result

    def create(self, item: T) -> T:
        for link in self.get_meta().links:
            link.before_create(item)
        result = self.get_store().create(item)
        if result:
            for link in self.get_meta().links:
                link.after_create(item)
        return result

    def _edit_batch(
        self, edits: List[BatchEdit[T, T]], items_by_key: Dict[str, T]
    ) -> List[BatchEditResult[T, T]]:
        meta = self.get_meta()
        self._before_edit_batch(edits, items_by_key)
        # pylint: disable=W0212
        results = self.get_store()._edit_batch(edits, items_by_key)
        for result in results:
            edit = result.edit
            if not result.success:
                continue
            if edit.create_item:
                for link in meta.links:
                    link.after_create(edit.create_item)
            elif edit.update_item:
                updates = edit.update_item
                key = meta.key_config.to_key_str(updates)
                item = items_by_key[key]
                for link in meta.links:
                    link.after_update(item, updates)
            else:
                item = items_by_key[edit.delete_key]
                for link in self.get_meta().links:
                    link.after_delete(item)
        return results

    def _before_edit_batch(
        self, edits: List[BatchEdit[T, T]], items_by_key: Dict[str, T]
    ):
        meta = self.get_meta()
        for edit in edits:
            if edit.create_item:
                for link in meta.links:
                    link.before_create(edit.create_item)
            elif edit.update_item:
                updates = edit.update_item
                key = meta.key_config.to_key_str(updates)
                item = items_by_key[key]
                for link in meta.links:
                    link.before_update(item, updates)
            else:
                item = items_by_key[edit.delete_key]
                for link in self.get_meta().links:
                    link.before_delete(item)

    def update_all(self, search_filter: SearchFilterABC[T], updates: T):
        StoreABC.update_all(
            self, search_filter, updates
        )  # Ensure data is loaded for checking

    def delete_all(self, search_filter: SearchFilterABC[T]):
        StoreABC.delete_all(self, search_filter)  # Ensure data is loaded for checking
