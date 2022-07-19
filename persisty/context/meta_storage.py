from dataclasses import field, dataclass
from itertools import islice
from typing import Iterator, List, Optional, Dict

from persisty.access_control.obj_access_control_abc import ObjAccessControlABC
from persisty.context.obj_storage_meta import (
    MetaStorageABC,
    CreateStorageMetaInput,
    UpdateStorageMetaInput,
    StorageMetaSearchFilter,
    StorageMetaSearchOrder,
)
from persisty.errors import PersistyError
from persisty.impl.mem.mem_storage import MemStorage
from persisty.storage.batch_edit import BatchEditABC, Create, Update, Delete
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.result_set import ResultSet
from persisty.storage.storage_meta import StorageMeta
from persisty.util import dataclass_to_params


@dataclass
class MetaStorage(MetaStorageABC):
    access_control: ObjAccessControlABC[
        StorageMeta,
        StorageMetaSearchFilter,
        CreateStorageMetaInput,
        UpdateStorageMetaInput,
    ]
    storage: Dict[str, MemStorage] = field(default_factory=dict)

    @property
    def batch_size(self) -> int:
        return 100

    def create(self, item: CreateStorageMetaInput) -> StorageMeta:
        params = dataclass_to_params(item)
        storage_meta = StorageMeta(**params)
        if storage_meta.name in self.storage:
            raise PersistyError(f"existing_value:{storage_meta.name}")
        storage = MemStorage(storage_meta)
        self.storage[storage_meta.name] = storage
        return storage_meta

    def read(self, key: str) -> Optional[StorageMeta]:
        storage = self.get_storage().get(key)
        return storage.storage_meta if storage else None

    def update(self, updates: UpdateStorageMetaInput) -> Optional[StorageMeta]:
        storage = self.get_storage().get(updates.name)
        if not storage:
            return None
        params = dataclass_to_params(storage.storage_meta)
        params.update(**dataclass_to_params(updates))
        storage_meta = StorageMeta(**params)
        storage.storage_meta = storage_meta
        return storage_meta

    def delete(self, key: str) -> bool:
        return bool(self.get_storage().pop(key, None))

    def search(
        self,
        search_filter_factory: Optional[StorageMetaSearchFilter] = None,
        search_order_factory: Optional[StorageMetaSearchOrder] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[StorageMeta]:
        items = iter(self.get_storage().values())
        items = (i.storage_meta for i in items)
        return meta_result_set(
            items, search_filter_factory, search_order_factory, page_key, limit
        )

    def search_all(
        self,
        search_filter_factory: Optional[StorageMetaSearchFilter] = None,
        search_order_factory: Optional[StorageMetaSearchOrder] = None,
    ) -> Iterator[StorageMeta]:
        page_key = None
        while True:
            result_set = self.search(
                search_filter_factory, search_order_factory, page_key
            )
            yield from result_set.results
            page_key = result_set.next_page_key
            if not page_key:
                return

    def count(self, search_filter: Optional[StorageMetaSearchFilter] = None) -> int:
        if not search_filter:
            return len(self.storage)
        count = sum(1 for _ in self.search_all(search_filter))
        return count

    def edit_batch(self, edits: List[BatchEditABC]) -> List[BatchEditResult]:
        return list(self.edit_all(edits))

    def edit_all(self, edits: Iterator[BatchEditABC]):
        for edit in edits:
            try:
                if isinstance(edit, Create):
                    item = self.create(edit.item)
                    yield BatchEditResult(edit, bool(item))
                elif isinstance(edit, Update):
                    item = self.update(edit.updates)
                    yield BatchEditResult(edit, bool(item))
                elif isinstance(edit, Delete):
                    deleted = self.delete(edit.key)
                    yield BatchEditResult(edit, bool(deleted))
                else:
                    yield BatchEditResult(
                        edit, False, "unsupported_edit_type", edit.__class__.__name__
                    )
            except Exception as e:
                yield BatchEditResult(edit, False, "exception", str(e))


def meta_result_set(
    items: Iterator[StorageMeta],
    search_filter_factory: StorageMetaSearchFilter,
    search_order_factory: StorageMetaSearchOrder,
    page_key: Optional[str],
    limit: int,
) -> ResultSet[StorageMeta]:
    if search_filter_factory and search_filter_factory.query:
        items = (
            i for i in items if search_filter_factory.query.lower() in i.name.lower()
        )
    if search_order_factory and search_order_factory.field:
        items = sorted(
            items,
            key=lambda i: getattr(i, search_order_factory.field.value),
            reverse=search_order_factory.desc,
        )
    if page_key:
        while True:
            if next(items).name == page_key:
                break
    items = list(islice(items, limit))
    page_key = None
    if len(items) == limit:
        page_key = items[-1].name
    return ResultSet(items, page_key)
