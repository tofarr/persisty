from typing import Optional, List, Iterator, Type

from dataclasses import dataclass

from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.types import ExternalItemType

from persisty.access_control.obj_access_control import ObjAccessControl
from persisty.cache_control.obj_cache_control import ObjCacheControl
from persisty.key_config.obj_key_config_abc import ObjKeyConfigABC
from persisty.obj_storage.obj_storage_abc import ObjStorageABC, T, F, S, C, U
from persisty.obj_storage.obj_storage_meta import ObjStorageMeta
from persisty.storage.batch_edit import BatchEditABC, Create, Update
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.storage.storage_abc import StorageABC


@dataclass(frozen=True)
class ObjStorage(ObjStorageABC[T, F, S, C, U]):
    storage: StorageABC
    item_marshaller: MarshallerABC[T]
    key_config: ObjKeyConfigABC[T]
    search_filter_factory_type: Type[F]
    search_order_factory_type: Type[S]
    create_input_marshaller: MarshallerABC[C]
    update_input_marshaller: MarshallerABC[U]

    @property
    def obj_storage_meta(self) -> ObjStorageMeta[T, F, S, C, U]:
        storage_meta = self.get_storage().get_storage_meta()
        return ObjStorageMeta(
            name=storage_meta.name,
            item_type=self.item_marshaller.marshalled_type,
            search_filter_factory_type=self.search_filter_factory_type,
            search_order_factory_type=self.search_order_factory_type,
            create_input_type=self.create_input_marshaller.marshalled_type,
            update_input_type=self.update_input_marshaller.marshalled_type,
            key_config=self.key_config,
            access_control=ObjAccessControl(storage_meta.access_control,
                                            self.item_marshaller,
                                            self.create_input_marshaller,
                                            self.update_input_marshaller),
            cache_control=ObjCacheControl(storage_meta.cache_control, self.item_marshaller),
            batch_size=storage_meta.batch_size
        )

    def create(self, item: C) -> T:
        dumped = self.create_input_marshaller.dump(item)
        created = self.get_storage().create(dumped)
        loaded = self.item_marshaller.load(created)
        return loaded

    def read(self, key: str) -> Optional[ExternalItemType]:
        read = self.get_storage().read(key)
        if read:
            loaded = self.item_marshaller.load(read)
            return loaded

    async def read_batch(self, keys: List[str]) -> List[Optional[T]]:
        read = await self.get_storage().read_batch(keys)
        loaded = [self.item_marshaller.load(item) if item else None for item in read]
        return loaded

    def read_all(self, keys: Iterator[str]) -> Iterator[Optional[T]]:
        for read in self.get_storage().read_all(keys):
            if read is None:
                yield read
            else:
                loaded = self.item_marshaller.load(read)
                yield loaded

    def update(self, updates: U) -> Optional[T]:
        dumped = self.update_input_marshaller.dump(updates)
        updated = self.get_storage().update(dumped)
        loaded = self.item_marshaller.load(updated)
        return loaded

    def delete(self, key: str) -> bool:
        return self.get_storage().delete(key)

    def search(self,
               search_filter_factory: Optional[F] = None,
               search_order_factory: Optional[S] = None,
               page_key: Optional[str] = None,
               limit: Optional[int] = None
               ) -> ResultSet[T]:
        search_filter = search_filter_factory.to_search_filter() if search_filter_factory else INCLUDE_ALL
        search_order = search_order_factory.to_search_order() if search_order_factory else None
        result_set = self.get_storage().search(search_filter, search_order, page_key, limit)
        result_set.results = [self.item_marshaller.load(item) if item else None for item in result_set.results]
        return result_set

    def search_all(self,
                   search_filter_factory: Optional[F] = None,
                   search_order_factory: Optional[S] = None
                   ) -> Iterator[ExternalItemType]:
        search_filter = search_filter_factory.to_search_filter() if search_filter_factory else INCLUDE_ALL
        search_order = search_order_factory.to_search_order() if search_order_factory else None
        for item in self.get_storage().search_all(search_filter, search_order):
            loaded = self.item_marshaller.load(item)
            yield loaded

    def count(self, search_filter_factory: Optional[F] = None) -> int:
        search_filter = search_filter_factory.to_search_filter() if search_filter_factory else INCLUDE_ALL
        return self.get_storage().count(search_filter)

    async def edit_batch(self, edits: List[BatchEditABC]) -> List[BatchEditResult]:
        """
        Do a batch edit and return a list of results. The results should contain all the same edits in the same
        order
        """
        unmarshalled_edits = list(self._edit_iterator(edits))
        results = await self.get_storage().edit_batch(unmarshalled_edits)
        for result, edit in zip(results, edits):
            result.edit = edit
        return results

    def edit_all(self, edits: Iterator[BatchEditABC]):
        edits = self._edit_iterator(edits)
        self.get_storage().edit_all(edits)

    def _edit_iterator(self, edits: Iterator[BatchEditABC]):
        edits = iter(edits)
        for edit in edits:
            if isinstance(edit, Create):
                edit = Create(self.create_input_marshaller.dump(edit.item), edit.id)
            elif isinstance(edit, Update):
                edit = Update(self.update_input_marshaller.dump(edit.updates), edit.id)
            yield edit
