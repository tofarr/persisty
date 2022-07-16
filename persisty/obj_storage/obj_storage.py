from typing import Optional, List, Iterator, Type

from dataclasses import dataclass

from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.types import ExternalItemType

from persisty.obj_storage.obj_storage_abc import ObjStorageABC, T, F, S, C, U
from persisty.storage.batch_edit import BatchEditABC, Create, Update
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.result_set import ResultSet
from persisty.storage.search_filter.include_all import INCLUDE_ALL
from persisty.storage.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.search_order import SearchOrder, NO_ORDER
from persisty.storage.storage_abc import StorageABC


@dataclass(frozen=True)
class ObjStorage(ObjStorageABC[T, F, S, C, U]):
    storage: StorageABC
    item_marshaller: MarshallerABC[T]
    search_filter_marshaller: MarshallerABC[F]
    search_order_marshaller: MarshallerABC[S]
    create_input_marshaller: MarshallerABC[C]
    update_input_marshaller: MarshallerABC[U]

    @property
    def item_type(self) -> Type[T]:
        return self.item_marshaller.marshalled_type

    @property
    def search_filter_type(self) -> Type[F]:
        return self.search_filter_marshaller.marshalled_type

    @property
    def search_order_type(self) -> Type[S]:
        return self.search_order_marshaller.marshalled_type

    @property
    def create_input_type(self) -> Type[C]:
        return self.create_input_marshaller.marshalled_type

    @property
    def update_input_type(self) -> Type[U]:
        return self.update_input_marshaller.marshalled_type

    @property
    def batch_size(self) -> int:
        return self.storage.storage_meta.batch_size

    def create(self, item: C) -> T:
        dumped = self.create_input_marshaller.dump(item)
        created = self.storage.create(dumped)
        loaded = self.item_marshaller.load(created)
        return loaded

    def read(self, key: str) -> Optional[ExternalItemType]:
        read = self.storage.read(key)
        if read:
            loaded = self.item_marshaller.load(read)
            return loaded

    async def read_batch(self, keys: List[str]) -> List[Optional[T]]:
        read = await self.storage.read_batch(keys)
        loaded = [self.item_marshaller.load(item) if item else None for item in read]
        return loaded

    def read_all(self, keys: Iterator[str]) -> Iterator[Optional[T]]:
        for read in self.storage.read_all(keys):
            if read is None:
                yield read
            else:
                loaded = self.item_marshaller.load(read)
                yield loaded

    def update(self, updates: U) -> Optional[T]:
        dumped = self.update_input_marshaller.dump(updates)
        updated = self.storage.update(dumped)
        loaded = self.item_marshaller.load(updated)
        return loaded

    def delete(self, key: str) -> bool:
        return self.storage.delete(key)

    def search(self,
               search_filter: Optional[F] = None,
               search_order: Optional[S] = None,
               page_key: Optional[str] = None,
               limit: Optional[int] = None
               ) -> ResultSet[T]:
        dumped_search_filter = self.search_filter_marshaller.dump(search_filter) if search_filter else INCLUDE_ALL
        dumped_search_order = self.search_order_marshaller.dump(search_order) if search_order else NO_ORDER
        result_set = self.storage.search(dumped_search_filter, dumped_search_order, page_key, limit)
        result_set.results = [self.item_marshaller.load(item) if item else None for item in result_set.results]
        return result_set

    def search_all(self,
                   search_filter: SearchFilterABC = INCLUDE_ALL,
                   search_order: SearchOrder = NO_ORDER
                   ) -> Iterator[ExternalItemType]:
        dumped_search_filter = self.search_filter_marshaller.dump(search_filter) if search_filter else INCLUDE_ALL
        dumped_search_order = self.search_order_marshaller.dump(search_order) if search_order else NO_ORDER
        for item in self.storage.search_all(dumped_search_filter, dumped_search_order):
            loaded = self.item_marshaller.load(item)
            yield loaded

    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        dumped_search_filter = self.search_filter_marshaller.dump(search_filter) if search_filter else INCLUDE_ALL
        return self.storage.count(dumped_search_filter)

    async def edit_batch(self, edits: List[BatchEditABC]) -> List[BatchEditResult]:
        """
        Do a batch edit and return a list of results. The results should contain all the same edits in the same
        order
        """
        unmarshalled_edits = list(self._edit_iterator(edits))
        results = await self.storage.edit_batch(unmarshalled_edits)
        for result, edit in zip(results, edits):
            result.edit = edit
        return results

    def edit_all(self, edits: Iterator[BatchEditABC]):
        edits = self._edit_iterator(edits)
        self.storage.edit_all(edits)

    def _edit_iterator(self, edits: Iterator[BatchEditABC]):
        edits = iter(edits)
        for edit in edits:
            if isinstance(edit, Create):
                edit = Create(self.create_input_marshaller.dump(edit.item), edit.id)
            elif isinstance(edit, Update):
                edit = Update(self.update_input_marshaller.dump(edit.updates), edit.id)
            yield edit
