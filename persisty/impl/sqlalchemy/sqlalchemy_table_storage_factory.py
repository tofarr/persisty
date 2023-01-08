from dataclasses import dataclass, field
from typing import Optional

from servey.security.authorization import Authorization

from persisty.impl.sqlalchemy.sqlalchemy_context import SqlalchemyContext
from persisty.impl.sqlalchemy.sqlalchemy_context_factory_abc import create_default_context
from persisty.impl.sqlalchemy.sqlalchemy_table_storage import SqlalchemyTableStorage
from persisty.storage.schema_validating_storage import SchemaValidatingStorage
from persisty.storage.secured_storage import secured_storage
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_factory_abc import StorageFactoryABC
from persisty.storage.storage_meta import StorageMeta


@dataclass
class SqlalchemyTableStorageFactory(StorageFactoryABC):
    storage_meta: StorageMeta
    context: SqlalchemyContext = field(default_factory=create_default_context)

    def get_storage_meta(self) -> StorageMeta:
        return self.storage_meta

    def create(self, authorization: Optional[Authorization]) -> Optional[StorageABC]:
        table = self.context.get_table(self.storage_meta)
        storage = SqlalchemyTableStorage(self.storage_meta, table, self.context.engine)
        storage = SchemaValidatingStorage(storage)
        storage = secured_storage(storage, authorization)
        return storage
