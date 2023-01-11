import os
from dataclasses import dataclass
from typing import Optional

from servey.servey_aws import is_lambda_env

from aaaa.impl.dynamodb.dynamodb_store_factory import DynamodbStoreFactory
from aaaa.impl.mem.mem_store_factory import MemStoreFactory
from aaaa.impl.sqlalchemy.sqlalchemy_table_store_factory import (
    SqlalchemyTableStoreFactory,
)
from aaaa.store.store_abc import StoreABC
from aaaa.store.store_factory_abc import StoreFactoryABC
from aaaa.store_meta import StoreMeta


@dataclass
class DefaultStoreFactory(StoreFactoryABC):
    store_meta: StoreMeta

    def get_meta(self) -> StoreMeta:
        return self.store_meta

    def create(self) -> Optional[StoreABC]:
        store = self.factory.create()
        return store

    @property
    def factory(self):
        factory = getattr(self, "_factory", None)
        if not factory:
            if os.environ.get("PERSISTY_SQL_URN"):
                factory = SqlalchemyTableStoreFactory(self.store_meta)
            elif is_lambda_env():
                factory = DynamodbStoreFactory(self.store_meta)
            else:
                from aaaa.io.seed import get_seed_data
                seed_items = get_seed_data(self.store_meta)
                key_config = self.store_meta.key_config
                seed_data = {key_config.to_key_str(i): i for i in seed_items}
                factory = MemStoreFactory(self.store_meta, seed_data)
            setattr(self, "_factory", factory)
        return factory
