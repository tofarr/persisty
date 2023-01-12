from dataclasses import dataclass, field
from typing import Optional

from persisty.impl.sqlalchemy.sqlalchemy_context import SqlalchemyContext
from persisty.impl.sqlalchemy.sqlalchemy_context_factory_abc import (
    create_default_context,
)
from persisty.impl.sqlalchemy.sqlalchemy_table_store import SqlalchemyTableStore
from persisty.store.restrict_access_store import restrict_access_store
from persisty.store.schema_validating_store import SchemaValidatingStore
from persisty.store.store_abc import StoreABC
from persisty.store.store_factory_abc import StoreFactoryABC
from persisty.store_meta import StoreMeta


@dataclass
class SqlalchemyTableStoreFactory(StoreFactoryABC):
    store_meta: StoreMeta
    context: SqlalchemyContext = field(default_factory=create_default_context)

    def get_meta(self) -> StoreMeta:
        return self.store_meta

    def create(self) -> Optional[StoreABC]:
        table = self.context.get_table(self.store_meta)
        store = SqlalchemyTableStore(self.store_meta, table, self.context.engine)
        store = SchemaValidatingStore(store)
        store = restrict_access_store(store, self.store_meta.store_access)
        return store
