import os
from dataclasses import dataclass
from typing import Optional

from servey.servey_aws import is_lambda_env

from persisty.impl.dynamodb.dynamodb_store_factory import DynamodbStoreFactory
from persisty.impl.mem.mem_store_factory import MemStoreFactory
from persisty.impl.sqlalchemy.sqlalchemy_table_store_factory import (
    SqlalchemyTableStoreFactory,
)
from persisty.store.store_abc import StoreABC, T
from persisty.store.wrapper_store_abc import WrapperStoreABC
from persisty.store_meta import StoreMeta


@dataclass
class DefaultStore(WrapperStoreABC[T]):
    meta: StoreMeta

    def get_meta(self) -> StoreMeta:
        return self.meta

    def get_store(self) -> StoreABC:
        store = getattr(self, '_store', None)
        if store:
            return store
        if os.environ.get("PERSISTY_SQL_URN"):
            factory = SqlalchemyTableStoreFactory(self.meta)
        elif is_lambda_env():
            factory = DynamodbStoreFactory(self.meta)
        else:
            from persisty.io.seed import get_seed_data
            seed_items = get_seed_data(self.meta)
            key_config = self.meta.key_config
            seed_data = {key_config.to_key_str(i): i for i in seed_items}
            factory = MemStoreFactory(self.meta, seed_data)
        store = factory.create()
        setattr(self, "_store", store)
        return store
