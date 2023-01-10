from dataclasses import dataclass, field
from typing import Optional

from persisty.impl.sqlalchemy.sqlalchemy_context import SqlalchemyContext
from persisty.impl.sqlalchemy.sqlalchemy_context_factory_abc import (
    create_default_context,
)
from persisty.impl.sqlalchemy.sqlalchemy_table_storage import SqlalchemyTableStorage
from persisty.storage.restrict_access_storage import restrict_access_storage
from persisty.storage.schema_validating_storage import SchemaValidatingStorage
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_factory_abc import StorageFactoryABC
from persisty.storage.storage_meta import StorageMeta
from persisty.trigger.wrapper import triggered_storage


@dataclass
class SqlalchemyTableStorageFactory(StorageFactoryABC):
    storage_meta: StorageMeta
    context: SqlalchemyContext = field(default_factory=create_default_context)

    def get_storage_meta(self) -> StorageMeta:
        return self.storage_meta

    def create(self) -> Optional[StorageABC]:
        table = self.context.get_table(self.storage_meta)
        storage = SqlalchemyTableStorage(self.storage_meta, table, self.context.engine)
        storage = SchemaValidatingStorage(storage)
        storage = restrict_access_storage(storage, self.storage_meta.storage_access)
        storage = triggered_storage(storage)
        return storage
