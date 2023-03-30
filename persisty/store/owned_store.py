import dataclasses
from typing import Optional, Iterator, List, Dict

from servey.security.authorization import Authorization

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.batch_edit import BatchEdit
from persisty.batch_edit_result import BatchEditResult
from persisty.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.store.store_abc import StoreABC, T
from persisty.store_meta import StoreMeta


@dataclasses.dataclass
class OwnedStore(StoreABC[T]):
    """
    Store which enforces ownership of items within it. Each item has an attribute
    which maps to the id of the owner, and access to read, update and delete operations
    can be optionally restricted
    """

    store: StoreABC[T]
    authorization: Authorization
    subject_id_attr_name: str = "subject_id"
    require_ownership_for_read: bool = False
    require_ownership_for_update: bool = True
    require_ownership_for_delete: bool = True

    def get_meta(self) -> StoreMeta:
        meta = getattr(self, '_meta', None)
        if meta is None:
            meta = meta_with_non_editable_subject_id(self.store.get_meta(), self.subject_id_attr_name)
            setattr(self, '_meta', meta)
        return meta

    def create(self, item: T) -> Optional[T]:
        setattr(item, self.subject_id_attr_name, self.authorization.subject_id)
        return self.store.create(item)

    def read(self, key: str) -> Optional[T]:
        item = self.store.read(key)
        if item and self.require_ownership_for_read:
            if (
                getattr(item, self.subject_id_attr_name)
                != self.authorization.subject_id
            ):
                return
        return item

    def _update(self, key: str, item: T, updates: T) -> Optional[T]:
        if self.require_ownership_for_update:
            if (
                getattr(item, self.subject_id_attr_name)
                != self.authorization.subject_id
            ):
                return
        setattr(updates, self.subject_id_attr_name, self.authorization.subject_id)
        return self.store._update(key, item, updates)

    def _delete(self, key: str, item: T) -> bool:
        if self.require_ownership_for_delete:
            if (
                getattr(item, self.subject_id_attr_name)
                != self.authorization.subject_id
            ):
                return False
        return self.store._delete(key, item)

    def count(self, search_filter: SearchFilterABC[T] = INCLUDE_ALL) -> int:
        if self.require_ownership_for_read:
            search_filter &= AttrFilter(
                self.subject_id_attr_name,
                AttrFilterOp.eq,
                self.authorization.subject_id,
            )
        return self.store.count(search_filter)

    def search(
        self,
        search_filter: SearchFilterABC[T] = INCLUDE_ALL,
        search_order: Optional[SearchOrder[T]] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[T]:
        if self.require_ownership_for_read:
            search_filter &= AttrFilter(
                self.subject_id_attr_name,
                AttrFilterOp.eq,
                self.authorization.subject_id,
            )
        return self.store.search(search_filter, search_order, page_key, limit)

    def search_all(
        self,
        search_filter: SearchFilterABC[T] = INCLUDE_ALL,
        search_order: Optional[SearchOrder[T]] = None,
    ) -> Iterator[T]:
        if self.require_ownership_for_read:
            search_filter &= AttrFilter(
                self.subject_id_attr_name,
                AttrFilterOp.eq,
                self.authorization.subject_id,
            )
        return self.store.search_all(search_filter, search_order)

    def _edit_batch(
        self, edits: List[BatchEdit[T, T]], items_by_key: Dict[str, T]
    ) -> List[BatchEditResult[T, T]]:
        filtered_edits = []
        to_key_str = self.get_meta().key_config.to_key_str
        for edit in edits:
            if edit.create_item:
                setattr(
                    edit.create_item,
                    self.subject_id_attr_name,
                    self.authorization.subject_id,
                )
                filtered_edits.append(edit)
            elif edit.update_item:
                key = to_key_str(edit.update_item)
                existing_item = items_by_key.get(key)
                existing_subject_id = getattr(existing_item, self.subject_id_attr_name)
                if (
                    self.require_ownership_for_update
                    and existing_subject_id != self.authorization.subject_id
                ):
                    continue
                else:
                    setattr(
                        edit.create_item,
                        self.subject_id_attr_name,
                        self.authorization.subject_id,
                    )
                    filtered_edits.append(edit)
            else:
                existing_item = items_by_key.get(edit.delete_key)
                existing_subject_id = getattr(existing_item, self.subject_id_attr_name)
                if (
                    self.require_ownership_for_delete
                    and existing_subject_id != self.authorization.subject_id
                ):
                    continue
                else:
                    filtered_edits.append(edit)
        filtered_results = self.store._edit_batch(filtered_edits, items_by_key)
        results_by_id = {r.edit.id: r for r in filtered_results}
        results = []
        for edit in edits:
            result = results_by_id.get(edit.id)
            if result:
                results.append(result)
            else:
                results.append(BatchEditResult(edit, False, "exception", "missing_key"))
        return results


def meta_with_non_editable_subject_id(meta: StoreMeta, subject_id_attr_name: str) -> StoreMeta:
    attrs = []
    for attr in meta.attrs:
        if attr.name == subject_id_attr_name:
            attr = dataclasses.replace(attr, creatable=False, updatable=False)
        attrs.append(attr)
    return dataclasses.replace(meta, attrs=attrs)
