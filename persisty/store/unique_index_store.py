from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.batch_edit import BatchEdit
from persisty.batch_edit_result import BatchEditResult
from persisty.errors import PersistyError
from persisty.index import Index
from persisty.search_filter.and_filter import And
from persisty.store.store_abc import StoreABC, T
from persisty.store.wrapper_store_abc import WrapperStoreABC
from persisty.util import UNDEFINED


@dataclass
class UniqueIndexStore(WrapperStoreABC[T]):
    store: StoreABC[T]
    unique_indexes: Tuple[Index, ...]

    def get_store(self) -> StoreABC[T]:
        return self.store

    def create(self, item: T) -> T:
        self._check_create(item)
        return self.get_store().create(item)

    def _update(self, key: str, item: T, updates: T) -> Optional[T]:
        self._check_update(key, item, updates)
        return self.get_store()._update(key, item, updates)

    def _edit_batch(
        self, edits: List[BatchEdit[T, T]], items_by_key: Dict[str, T]
    ) -> List[BatchEditResult[T, T]]:
        filtered_edits = []
        key_config = self.store.get_meta().key_config
        for edit in edits:
            if edit.create_item:
                self._check_create(edit.create_item)
                filtered_edits.append(edit)
            elif edit.update_item:
                key = key_config.to_key_str(edit.update_item)
                item = items_by_key[key]
                updates = edit.update_item
                self._check_update(key, item, updates)
                filtered_edits.append(edit)
            else:
                filtered_edits.append(edit)
        return self.get_store()._edit_batch(edits, items_by_key)

    def _check_create(self, item: T):
        store = self.get_store()
        for index in self.unique_indexes:
            search_filter = And(tuple(
                AttrFilter(a, AttrFilterOp.eq, getattr(item, a))
                for a in index.attr_names
            ))
            item = next(store.search_all(search_filter), None)
            if item:
                raise PersistyError('non_unique_item')

    def _check_update(self, key: str, item: T, updates: T):
        store = self.get_store()
        key_config = store.get_meta().key_config
        for index in self.unique_indexes:
            search_filters = []
            for attr_name in index.attr_names:
                value = getattr(updates, attr_name)
                if value is UNDEFINED:
                    value = getattr(item, attr_name)
                search_filters.append(AttrFilter(attr_name, AttrFilterOp.eq, value))
            search_filter = And(search_filters)
            results = (item for item in store.search_all(search_filter) if key_config.to_key_str(item) != key)
            item = next(results, None)
            if item:
                raise PersistyError('non_unique_item')


def unique_index_store(store: StoreABC):
    meta = store.get_meta()
    unique_indexes = tuple(i for i in meta.indexes if i.unique)
    if unique_indexes:
        return UniqueIndexStore(store, unique_indexes)
    return store
