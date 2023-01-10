import os
from dataclasses import dataclass
from typing import Optional

from servey.security.authorization import Authorization
from servey.servey_aws import is_lambda_env

from persisty.impl.dynamodb.dynamodb_storage_factory import DynamodbStorageFactory
from persisty.impl.mem.mem_storage_factory import MemStorageFactory
from persisty.impl.sqlalchemy.sqlalchemy_table_storage_factory import (
    SqlalchemyTableStorageFactory,
)
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_factory_abc import StorageFactoryABC
from persisty.storage.storage_meta import StorageMeta


@dataclass
class DefaultStorageFactory(StorageFactoryABC):
    storage_meta: StorageMeta

    def get_storage_meta(self) -> StorageMeta:
        return self.storage_meta

    def create(self) -> Optional[StorageABC]:
        storage = self.factory.create()
        return storage

    @property
    def factory(self):
        factory = getattr(self, "_factory", None)
        if not factory:
            if os.environ.get("PERSISTY_SQL_URN"):
                factory = SqlalchemyTableStorageFactory(self.storage_meta)
            elif is_lambda_env():
                factory = DynamodbStorageFactory(self.storage_meta)
            else:
                from persisty.io.seed import get_seed_data
                seed_items = get_seed_data(self.storage_meta)
                key_config = self.storage_meta.key_config
                seed_data = {key_config.to_key_str(i): i for i in seed_items}
                factory = MemStorageFactory(self.storage_meta, seed_data)
            setattr(self, "_factory", factory)
        return factory
