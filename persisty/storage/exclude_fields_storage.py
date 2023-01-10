import dataclasses
from dataclasses import dataclass
from typing import FrozenSet, Optional, List, Dict, Iterator

from marshy.types import ExternalItemType

from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.storage.batch_edit import BatchEdit
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.result_set import ResultSet
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta
from persisty.storage.wrapper_storage_abc import WrapperStorageABC


@dataclass
class ExcludeFieldsStorage(WrapperStorageABC):
    """
    Storage which wraps another and removes a reference to an attribute. It assumes that these values may be created
    automatically or that create operations are not permitted
    """
    storage: StorageABC
    exclude_field_names: FrozenSet[str]

    def get_storage(self) -> StorageABC:
        return self.storage

    def get_storage_meta(self) -> StorageMeta:
        storage_meta = getattr(self, '_storage_meta', None)
        if not storage_meta:
            storage_meta = self.storage.get_storage_meta()
            storage_meta = dataclasses.replace(
                storage_meta,
                fields=tuple(
                    f for f in storage_meta.fields
                    if f.name not in self.exclude_field_names
                )
            )
        return storage_meta
    
    def create(self, item: ExternalItemType) -> ExternalItemType:
        return self.get_storage().create(self._filter_item(item))

    def read(self, key: str) -> Optional[ExternalItemType]:
        return self._filter_item(self.get_storage().read(key))

    def read_batch(self, keys: List[str]) -> List[Optional[ExternalItemType]]:
        items = self.get_storage().read_batch(keys)
        items = [self._filter_item(i) for i in items]
        return items
    
    def update(
        self, updates: ExternalItemType, precondition: SearchFilterABC = INCLUDE_ALL
    ) -> Optional[ExternalItemType]:
        return self.storage.update(
            self._filter_item(updates),
            self._filter_search_filter(precondition)
        )

    def search(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[ExternalItemType]:
        return self.get_storage().search(
            self._filter_search_filter(search_filter),
            self._filter_search_order(search_order),
            page_key,
            limit
        )

    def search_all(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
    ) -> Iterator[ExternalItemType]:
        return self.get_storage().search_all(
            self._filter_search_filter(search_filter),
            self._filter_search_order(search_order)
        )

    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        return self.get_storage().count(self._filter_search_filter(search_filter),)

    def _edit_batch(
        self, edits: List[BatchEdit], items_by_key: Dict[str, ExternalItemType]
    ) -> List[BatchEditResult]:
        edits = [self._filter_batch_edit(e) for e in edits]
        return self.get_storage()._edit_batch(edits, items_by_key)

    def edit_all(self, edits: Iterator[BatchEdit]) -> Iterator[BatchEditResult]:
        edits = (self._filter_batch_edit(e) for e in edits)
        return self.get_storage().edit_all(edits)

    def _filter_item(self, item: Optional[ExternalItemType]) -> Optional[ExternalItemType]:
        if item:
            item = {k: v for k, v in item.items() if k not in self.exclude_field_names}
        return item
    
    def _filter_batch_edit(self, edit: BatchEdit) -> BatchEdit:
        return BatchEdit(
            id=edit.id,
            create_item=self._filter_item(edit.create_item),
            update_item=self._filter_item(edit.update_item),
            delete_key=edit.delete_key
        )

    def _filter_search_order(self, search_order: Optional[SearchOrder]) -> Optional[SearchOrder]:
        if search_order:
            orders = tuple(s for s in search_order.orders if s.field not in self.exclude_field_names)
            if orders:
                return SearchOrder(orders)

    def _filter_search_filter(self, search_filter: SearchFilterABC):
        search_filter = search_filter.lock_fields(self.get_storage_meta().fields)
        return search_filter
