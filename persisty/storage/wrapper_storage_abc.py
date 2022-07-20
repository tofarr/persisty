from abc import ABC, abstractmethod
from typing import Iterator, List, Optional

from marshy.types import ExternalItemType

from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.batch_edit import BatchEditABC
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.result_set import ResultSet
from persisty.search_order.search_order import SearchOrder
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta
from persisty.util import get_logger

logger = get_logger(__name__)


class WrapperStorageABC(StorageABC, ABC):
    @abstractmethod
    def get_storage(self) -> StorageABC:
        """Get wrapped storage"""

    def get_storage_meta(self) -> StorageMeta:
        return self.get_storage().get_storage_meta()

    def create(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        return self.get_storage().create(item)

    def read(self, key: str) -> Optional[ExternalItemType]:
        return self.get_storage().read(key)

    def read_batch(self, keys: List[str]) -> List[Optional[ExternalItemType]]:
        return self.get_storage().read_batch(keys)

    def update(
        self, updates: ExternalItemType, search_filter: SearchFilterABC = INCLUDE_ALL
    ) -> Optional[ExternalItemType]:
        return self.get_storage().update(updates, search_filter)

    def delete(self, key: str) -> bool:
        return self.get_storage().delete(key)

    def search(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[ExternalItemType]:
        return self.get_storage().search(search_filter, search_order, page_key, limit)

    def search_all(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
    ) -> Iterator[ExternalItemType]:
        return self.get_storage().search_all(search_filter, search_order)

    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        return self.get_storage().count(search_filter)

    def edit_batch(self, edits: List[BatchEditABC]):
        return self.edit_batch(edits)

    def edit_all(self, edits: Iterator[BatchEditABC]) -> Iterator[BatchEditResult]:
        return self.get_storage().edit_all(edits)
