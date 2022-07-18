from abc import ABC, abstractmethod
from typing import Iterator, List, Optional

from marshy.types import ExternalItemType

from persisty.storage.batch_edit import BatchEditABC
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.result_set import ResultSet
from persisty.storage.search_filter import SearchFilterABC, INCLUDE_ALL
from persisty.storage.search_order import SearchOrderABC
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta


class WrapperStorageABC(StorageABC, ABC):

    @abstractmethod
    @property
    def storage(self) -> StorageABC:
        """ Get the wrapped storage object """

    @property
    def storage_meta(self) -> StorageMeta:
        return self.storage.storage_meta

    def create(self, item: ExternalItemType) -> ExternalItemType:
        return self.storage.create(item)

    def read(self, key: str) -> Optional[ExternalItemType]:
        return self.storage.read(key)

    async def read_batch(self, keys: List[str]) -> List[Optional[ExternalItemType]]:
        return await self.storage.read_batch(keys)

    def read_all(self, keys: Iterator[str]) -> Iterator[Optional[ExternalItemType]]:
        return self.storage.read_all(keys)

    def update(self, updates: ExternalItemType) -> Optional[ExternalItemType]:
        return self.storage.update(updates)

    def delete(self, key: str) -> bool:
        return self.storage.delete(key)

    def search(self,
               search_filter: SearchFilterABC = INCLUDE_ALL,
               search_order: Optional[SearchOrderABC] = None,
               page_key: Optional[str] = None,
               limit: Optional[int] = None
               ) -> ResultSet[ExternalItemType]:
        return self.storage.search(search_filter, search_order, page_key, limit)

    def search_all(self,
                   search_filter: SearchFilterABC = INCLUDE_ALL,
                   search_order: Optional[SearchOrderABC] = None
                   ) -> Iterator[ExternalItemType]:
        return self.storage.search_all(search_filter, search_order)

    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        return self.storage.count(search_filter)

    async def edit_batch(self, edits: List[BatchEditABC]) -> List[BatchEditResult]:
        return await self.storage.edit_batch(edits)

    def edit_all(self, edits: Iterator[BatchEditABC]) -> Iterator[BatchEditResult]:
        return self.storage.edit_all(edits)
