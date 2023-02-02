from abc import ABC, abstractmethod
from typing import Iterator, List, Optional, Dict

from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.batch_edit import BatchEdit
from persisty.batch_edit_result import BatchEditResult
from persisty.result_set import ResultSet
from persisty.search_order.search_order import SearchOrder
from persisty.store.store_abc import StoreABC, T
from persisty.stored import StoreMeta
from persisty.util import get_logger

logger = get_logger(__name__)


class WrapperStoreABC(StoreABC[T], ABC):
    @abstractmethod
    def get_store(self) -> StoreABC:
        """Get wrapped store"""

    def get_meta(self) -> StoreMeta:
        return self.get_store().get_meta()

    def create(self, item: T) -> T:
        return self.get_store().create(item)

    def read(self, key: str) -> Optional[T]:
        return self.get_store().read(key)

    def read_batch(self, keys: List[str]) -> List[Optional[T]]:
        return self.get_store().read_batch(keys)

    def _update(self, key: str, item: T, updates: T) -> Optional[T]:
        return self.get_store()._update(key, item, updates)

    def _delete(self, key: str, item: T) -> bool:
        return self.get_store()._delete(key, item)

    def search(
        self,
        search_filter: SearchFilterABC[T] = INCLUDE_ALL,
        search_order: Optional[SearchOrder[T]] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[T]:
        store = self.get_store()
        return store.search(search_filter, search_order, page_key, limit)

    def search_all(
        self,
        search_filter: SearchFilterABC[T] = INCLUDE_ALL,
        search_order: Optional[SearchOrder[T]] = None,
    ) -> Iterator[T]:
        return self.get_store().search_all(search_filter, search_order)

    def count(self, search_filter: SearchFilterABC[T] = INCLUDE_ALL) -> int:
        return self.get_store().count(search_filter)

    def _edit_batch(
        self, edits: List[BatchEdit[T, T]], items_by_key: Dict[str, T]
    ) -> List[BatchEditResult[T, T]]:
        return self.get_store()._edit_batch(edits, items_by_key)

    def edit_all(
        self, edits: Iterator[BatchEdit[T, T]]
    ) -> Iterator[BatchEditResult[T, T]]:
        return self.get_store().edit_all(edits)
