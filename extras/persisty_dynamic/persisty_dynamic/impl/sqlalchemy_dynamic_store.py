from dataclasses import dataclass, field
from typing import Optional

from servey.security.authorization import Authorization

from persisty.impl.sqlalchemy.sqlalchemy_context import SqlalchemyContext
from persisty.impl.sqlalchemy.sqlalchemy_context_factory_abc import (
    create_default_context,
)
from persisty.impl.sqlalchemy.sqlalchemy_table_store_factory import (
    SqlalchemyTableStoreFactory,
)
from persisty.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.store.store_abc import StoreABC
from persisty_dynamic.dynamic_store_abc import DynamicStoreABC, DynamicStoreFactoryABC
from persisty_dynamic.dynamic_store_meta import DynamicStoreMeta


@dataclass
class SqlalchemyDynamicStore(DynamicStoreABC):
    """
    Dynamic store which will create / delete tables as required in dynamodb
    """

    store: StoreABC[DynamicStoreMeta]
    table_name_pattern: str = "dynamic_{name}"
    context: SqlalchemyContext = field(default_factory=create_default_context)

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
        dynamic_store_meta = self.read(name)
        if not dynamic_store_meta:
            return
        meta = dynamic_store_meta.to_meta()
        factory = SqlalchemyTableStoreFactory(meta, self.context)
        store = factory.create()
        return store

    def create(self, item: DynamicStoreMeta) -> Optional[DynamicStoreMeta]:
        result = self.store.create(item)
        if result:
            meta = result.to_meta()
            table = self.context.get_table(meta)
            table.create()
        return result

    def _delete(self, key: str, item: DynamicStoreMeta) -> bool:
        result = self.store._delete(key, item)
        if result:
            meta = item.to_meta()
            table = self.context.get_table(meta)
            table.drop()
        return result


class SqlalchemyDynamicStoreFactory(DynamicStoreFactoryABC):
    def create(self, store: StoreABC[DynamicStoreMeta]) -> Optional[DynamicStoreABC]:
        return SqlalchemyDynamicStore(store)
