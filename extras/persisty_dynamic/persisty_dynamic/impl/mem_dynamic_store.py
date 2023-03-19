from dataclasses import dataclass, field
from typing import Optional, Dict

from servey.security.authorization import Authorization

from persisty.impl.mem.mem_store import MemStore
from persisty.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.store.store_abc import StoreABC
from persisty_dynamic.dynamic_store_abc import DynamicStoreABC, DynamicStoreFactoryABC
from persisty_dynamic.dynamic_store_meta import DynamicStoreMeta


@dataclass
class MemDynamicStore(DynamicStoreABC):
    """
    Dynamic store which will create / delete tables as required in memory. Mostly useful for testing
    """

    store: StoreABC[DynamicStoreMeta]
    stores: Dict[str, StoreABC] = field(default_factory=dict)

    def read(self, key: str) -> Optional[DynamicStoreMeta]:
        return self.store.read(key)

    def _update(
        self, key: str, item: DynamicStoreMeta, updates: DynamicStoreMeta
    ) -> Optional[DynamicStoreMeta]:
        raise ValueError("not_implemented")

    def count(
        self, search_filter: SearchFilterABC[DynamicStoreMeta] = INCLUDE_ALL
    ) -> int:
        return self.store.count(search_filter)

    def search(
        self,
        search_filter: SearchFilterABC[DynamicStoreMeta] = INCLUDE_ALL,
        search_order: Optional[SearchOrder[DynamicStoreMeta]] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[DynamicStoreMeta]:
        return self.store.search(search_filter, search_order, page_key, limit)

    def get_store(
        self, name: str, authorization: Optional[Authorization]
    ) -> Optional[StoreABC]:
        return self.stores.get(name)

    def create(self, item: DynamicStoreMeta) -> Optional[DynamicStoreMeta]:
        result = self.store.create(item)
        if result:
            store_meta = result.to_meta()
            self.stores[result.name] = MemStore(store_meta, {})
        return result

    def _delete(self, key: str, item: DynamicStoreMeta) -> bool:
        result = self.store._delete(key, item)
        if result:
            del self.stores[key]
        return result


class MemDynamicStoreFactory(DynamicStoreFactoryABC):
    def create(self, store: StoreABC[DynamicStoreMeta]) -> Optional[DynamicStoreABC]:
        return MemDynamicStore(store)
