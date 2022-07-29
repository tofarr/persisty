from dataclasses import dataclass
from typing import Optional, List, Iterator, Generic

from marshy.types import ExternalItemType

from persisty.obj_storage.obj_storage_meta import ObjStorageMeta, T, F, S, C, U
from persisty.storage.batch_edit import BatchEditABC, Create, Update
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.storage.storage_abc import StorageABC


@dataclass(frozen=True)
class ObjStorage(Generic[T, F, S, C, U]):
    storage: StorageABC
    obj_storage_meta: ObjStorageMeta[T, F, S, C, U]

    def create(self, create_input: C) -> T:
        obj_storage_meta = self.obj_storage_meta
        dumped = obj_storage_meta.dump_create_input(create_input)
        created = self.storage.create(dumped)
        loaded = obj_storage_meta.load_item(created)
        return loaded

    def read(self, key: str) -> Optional[ExternalItemType]:
        read = self.storage.read(key)
        if read:
            loaded = self.obj_storage_meta.load_item(read)
            return loaded

    def read_batch(self, keys: List[str]) -> List[Optional[T]]:
        read = self.storage.read_batch(keys)
        marshaller = self.obj_storage_meta.item_marshaller
        loaded = [marshaller.load(i) if i else None for i in read]
        return loaded

    def read_all(self, keys: Iterator[str]) -> Iterator[Optional[T]]:
        marshaller = self.obj_storage_meta.item_marshaller
        for read in self.storage.read_all(keys):
            if read is None:
                yield read
            else:
                loaded = marshaller.load(read)
                yield loaded

    def update(self, updates: U) -> Optional[T]:
        obj_storage_meta = self.obj_storage_meta
        dumped = obj_storage_meta.dump_update_input(updates)
        updated = self.storage.update(dumped)
        loaded = obj_storage_meta.load_item(updated)
        return loaded

    def delete(self, key: str) -> bool:
        return self.storage.delete(key)

    def search(
        self,
        search_filter_factory: Optional[F] = None,
        search_order_factory: Optional[S] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[T]:
        search_filter = (
            search_filter_factory.to_search_filter()
            if search_filter_factory
            else INCLUDE_ALL
        )
        search_order = (
            search_order_factory.to_search_order() if search_order_factory else None
        )
        result_set = self.storage.search(search_filter, search_order, page_key, limit)
        marshaller = self.obj_storage_meta.item_marshaller
        result_set.results = [
            marshaller.load(item) if item else None for item in result_set.results
        ]
        return result_set

    def search_all(
        self,
        search_filter_factory: Optional[F] = None,
        search_order_factory: Optional[S] = None,
    ) -> Iterator[ExternalItemType]:
        search_filter = (
            search_filter_factory.to_search_filter()
            if search_filter_factory
            else INCLUDE_ALL
        )
        search_order = (
            search_order_factory.to_search_order() if search_order_factory else None
        )
        marshaller = self.obj_storage_meta.item_marshaller
        for item in self.storage.search_all(search_filter, search_order):
            loaded = marshaller.load(item)
            yield loaded

    def count(self, search_filter_factory: Optional[F] = None) -> int:
        search_filter = (
            search_filter_factory.to_search_filter()
            if search_filter_factory
            else INCLUDE_ALL
        )
        return self.storage.count(search_filter)

    def edit_batch(self, edits: List[BatchEditABC]) -> List[BatchEditResult]:
        """
        Do a batch edit and return a list of results. The results should contain all the same edits in the same
        order
        """
        unmarshalled_edits = list(self._edit_iterator(edits))
        results = self.storage.edit_batch(unmarshalled_edits)
        for result, edit in zip(results, edits):
            result.edit = edit
        return results

    def edit_all(self, edits: Iterator[BatchEditABC]):
        edits = self._edit_iterator(edits)
        self.storage.edit_all(edits)

    def _edit_iterator(self, edits: Iterator[BatchEditABC]):
        edits = iter(edits)
        create_marshaller = self.obj_storage_meta.create_input_marshaller
        update_marshaller = self.obj_storage_meta.update_input_marshaller
        for edit in edits:
            if isinstance(edit, Create):
                edit = Create(create_marshaller.dump(edit.item), edit.id)
            elif isinstance(edit, Update):
                edit = Update(update_marshaller.dump(edit.updates), edit.id)
            yield edit
