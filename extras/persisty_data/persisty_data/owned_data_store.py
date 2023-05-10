from dataclasses import dataclass
from typing import Optional, Iterator, Dict, List

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.batch_edit import BatchEdit
from persisty.batch_edit_result import BatchEditResult
from persisty.errors import PersistyError
from persisty.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.store.wrapper_store_abc import WrapperStoreABC
from servey.security.authorization import Authorization

from persisty_data.data_item_abc import DataItemABC
from persisty_data.data_store_abc import DataStoreABC


@dataclass
class OwnedDataStore(DataStoreABC, WrapperStoreABC[DataItemABC]):
    store: DataStoreABC
    authorization: Optional[Authorization]
    require_owner_for_read: bool = False

    def get_store(self) -> DataStoreABC:
        return self.store

    def get_prefix(self):
        return self.authorization.subject_id + "/"

    def create(self, item: DataItemABC) -> Optional[DataItemABC]:
        prefix = self.get_prefix()
        if not item.key.startswith(prefix):
            raise PersistyError(
                f"invalid_key:must_start_with:{self.authorization.subject_id}"
            )
        return self.store.create(item)

    def read(self, key: str) -> Optional[DataItemABC]:
        if self.require_owner_for_read and not key.startswith(self.get_prefix()):
            return
        item = self.store.read(key)
        return item

    def _update(
        self, key: str, item: DataItemABC, updates: DataItemABC
    ) -> Optional[DataItemABC]:
        self._check_key_for_edit(key)
        return self.store._update(key, item, updates)

    def _delete(self, key: str, item: DataItemABC) -> bool:
        self._check_key_for_edit(key)
        return self.store._delete(key, item)

    def count(self, search_filter: SearchFilterABC[DataItemABC] = INCLUDE_ALL) -> int:
        if self.require_owner_for_read:
            search_filter &= AttrFilter(
                "key", AttrFilterOp.startswith, self.get_prefix()
            )
        return self.store.count(search_filter)

    def search(
        self,
        search_filter: SearchFilterABC[DataItemABC] = INCLUDE_ALL,
        search_order: Optional[SearchOrder[DataItemABC]] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[DataItemABC]:
        if self.require_owner_for_read:
            search_filter &= AttrFilter(
                "key", AttrFilterOp.startswith, self.get_prefix()
            )
        return self.store.search(search_filter, search_order, page_key, limit)

    def search_all(
        self,
        search_filter: SearchFilterABC[DataItemABC] = INCLUDE_ALL,
        search_order: Optional[SearchOrder[DataItemABC]] = None,
    ) -> Iterator[DataItemABC]:
        if self.require_owner_for_read:
            search_filter &= AttrFilter(
                "key", AttrFilterOp.startswith, self.get_prefix()
            )
        return self.store.search_all(search_filter, search_order)

    def _edit_batch(
        self,
        edits: List[BatchEdit[DataItemABC, DataItemABC]],
        items_by_key: Dict[str, DataItemABC],
    ) -> List[BatchEditResult[DataItemABC, DataItemABC]]:
        filtered_edits = []
        for edit in edits:
            if edit.create_item:
                if edit.update_item.key.startswith(self.get_prefix()):
                    filtered_edits.append(edit)
            elif edit.update_item:
                if edit.update_item.key.startswith(self.get_prefix()):
                    filtered_edits.append(edit)
            else:
                if edit.delete_key.startswith(self.get_prefix()):
                    filtered_edits.append(edit)
        filtered_results = self.store._edit_batch(filtered_edits, items_by_key)
        results_by_id = {r.edit.id: r for r in filtered_results}
        results = []
        for edit in edits:
            result = results_by_id.get(edit.id)
            if result:
                results.append(result)
            else:
                results.append(BatchEditResult(edit, False, "exception", "invalid_key"))
        return results

    def get_data_writer(self, key: str, content_type: Optional[str] = None):
        self._check_key_for_edit(key)
        return self.store.get_data_writer(key, content_type)

    def copy_data_from(self, source: DataItemABC):
        self._check_key_for_edit(source.key)
        return self.store.copy_data_from(source)

    def _check_key_for_edit(self, key: str):
        prefix = self.get_prefix()
        if not key.startswith(prefix):
            raise PersistyError(f"invalid_key:must_start_with:{prefix}")
