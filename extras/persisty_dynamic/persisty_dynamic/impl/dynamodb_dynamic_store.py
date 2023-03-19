from dataclasses import dataclass
from typing import Optional

import boto3
from servey.security.authorization import Authorization
from servey.servey_aws import is_lambda_env

from persisty.impl.dynamodb.dynamodb_store_factory import DynamodbStoreFactory
from persisty.impl.dynamodb.dynamodb_table_store import DynamodbTableStore
from persisty.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.store.store_abc import StoreABC
from persisty.store_meta import get_meta
from persisty.util import filter_none
from persisty_dynamic.dynamic_store_abc import DynamicStoreABC, DynamicStoreFactoryABC
from persisty_dynamic.dynamic_store_meta import DynamicStoreMeta


@dataclass
class DynamodbDynamicStore(DynamicStoreABC):
    """
    Dynamic store which will create / delete tables as required in dynamodb
    """

    store: StoreABC[DynamicStoreMeta]
    table_name_pattern: str = "dynamic_{name}"
    aws_profile_name: str = None
    region_name: str = None

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
        if dynamic_store_meta:
            factory = self._get_dynamodb_table_store_factory(dynamic_store_meta)
            store = factory.create()
            return store

    def _get_dynamodb_table_store_factory(
        self, dynamic_store_meta: DynamicStoreMeta
    ) -> DynamodbStoreFactory:
        factory = DynamodbStoreFactory(meta=dynamic_store_meta.to_meta())
        factory.table_name = self.table_name_pattern.format(
            name=dynamic_store_meta.name
        )
        factory.derive_from_meta()
        return factory

    def create(self, item: DynamicStoreMeta) -> Optional[DynamicStoreMeta]:
        result = self.store.create(item)
        if result:
            factory = self._get_dynamodb_table_store_factory(result)
            factory.create_table_in_aws()
        return result

    def _delete(self, key: str, item: DynamicStoreMeta) -> bool:
        result = self.store._delete(key, item)
        if result:
            self._dynamodb_client().delete_table(
                TableName=self.table_name_pattern.format(name=item.name)
            )
        return result

    def _dynamodb_client(self):
        if hasattr(self, "_client"):
            return self._client
        kwargs = filter_none(
            dict(profile_name=self.aws_profile_name, region_name=self.region_name)
        )
        session = boto3.Session(**kwargs)
        client = session.client("dynamodb")
        object.__setattr__(self, "_client", client)
        return client


class DynamodbDynamicStoreFactory(DynamicStoreFactoryABC):
    def create(self, store: StoreABC[DynamicStoreMeta]) -> Optional[DynamicStoreABC]:
        if is_lambda_env():
            return DynamodbDynamicStore(store)
