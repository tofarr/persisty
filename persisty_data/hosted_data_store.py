from dataclasses import dataclass
from typing import Optional, Callable, Iterator

from servey.security.authorization import Authorization

from persisty.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.store_meta import StoreMeta
from persisty_data.data_item_abc import DataItemABC
from persisty_data.data_store_abc import DataStoreABC


@dataclass
class HostedDataStore(DataStoreABC):
    data_store: DataStoreABC
    get_download_url: Callable[[str, Optional[Authorization]], str]
    authorization: Optional[Authorization]

    def get_data_writer(self, key: str, content_type: Optional[str] = None):
        return self.data_store.get_data_writer(key, content_type)

    def get_meta(self) -> StoreMeta:
        return self.data_store.get_meta()

    def create(self, item: DataItemABC) -> Optional[DataItemABC]:
        item = self.data_store.create(item)
        item.data_url = self.get_download_url(item.key, self.authorization)
        return item

    def read(self, key: str) -> Optional[DataItemABC]:
        item = self.data_store.read(key)
        if item:
            # noinspection PyPropertyAccess
            item.data_url = self.get_download_url(item.key, self.authorization)
            return item

    def _update(self, key: str, item: DataItemABC, updates: DataItemABC) -> Optional[DataItemABC]:
        item = self.data_store._update(key, item, updates)
        item.data_url = self.get_download_url(item.key, self.authorization)
        return item

    def _delete(self, key: str, item: DataItemABC) -> bool:
        return self.data_store._delete(key, item)

    def count(self, search_filter: SearchFilterABC[DataItemABC] = INCLUDE_ALL) -> int:
        return self.data_store.count(search_filter)

    def search(
        self,
        search_filter: SearchFilterABC[DataItemABC] = INCLUDE_ALL,
        search_order: Optional[SearchOrder[DataItemABC]] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[DataItemABC]:
        result_set = self.data_store.search(search_filter, search_order, page_key, limit)
        for item in result_set.results:
            # noinspection PyPropertyAccess
            item.data_url = self.get_download_url(item.key, self.authorization)
        return result_set

    def search_all(
        self,
        search_filter: SearchFilterABC[DataItemABC] = INCLUDE_ALL,
        search_order: Optional[SearchOrder[DataItemABC]] = None,
    ) -> Iterator[DataItemABC]:
        for item in self.data_store.search_all(search_filter, search_order):
            # noinspection PyPropertyAccess
            item.data_url = self.get_download_url(item.key, self.authorization)
            yield item
