import os
from dataclasses import dataclass, field
from typing import Dict

from servey.servey_aws import is_lambda_env

from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.impl.mem.mem_store_factory import MemStoreFactory
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta


@dataclass(frozen=True)
class StoreFactory(StoreFactoryABC):
    cache: Dict[str, StoreABC] = field(default_factory=dict)

    def create(self, store_meta: StoreMeta) -> StoreABC:
        store = self.cache.get(store_meta.name)
        if store:
            return store
        if os.environ.get("PERSISTY_SQL_URN"):
            from persisty.impl.sqlalchemy.sqlalchemy_table_store_factory import (
                SqlalchemyTableStoreFactory,
            )

            factory = SqlalchemyTableStoreFactory()
        elif is_lambda_env():
            from persisty.impl.dynamodb.dynamodb_store_factory import (
                DynamodbStoreFactory,
            )

            factory = DynamodbStoreFactory()
            factory.derive_from_meta(store_meta)
        else:
            from persisty.io.seed import get_seed_items

            seed_items = get_seed_items(store_meta)
            key_config = store_meta.key_config
            seed_data = {key_config.to_key_str(i): i for i in seed_items}
            factory = MemStoreFactory(seed_data)
        store = factory.create(store_meta)
        self.cache[store_meta.name] = store
        return store
