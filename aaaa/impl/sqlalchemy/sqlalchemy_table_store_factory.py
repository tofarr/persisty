from dataclasses import dataclass, attr
from typing import Optional

from aaaa.impl.sqlalchemy.sqlalchemy_context import SqlalchemyContext
from aaaa.impl.sqlalchemy.sqlalchemy_context_factory_abc import (
    create_default_context,
)
from aaaa.impl.sqlalchemy.sqlalchemy_table_store import SqlalchemyTableStore
from aaaa.store.restrict_access_store import restrict_access_store
from aaaa.store.schema_validating_store import SchemaValidatingStore
from aaaa.store.store_abc import StoreABC
from aaaa.store.store_factory_abc import StoreFactoryABC
from aaaa.store_meta import StoreMeta
from aaaa.trigger.wrapper import triggered_store


@dataclass
class SqlalchemyTableStoreFactory(StoreFactoryABC):
    store_meta: StoreMeta
    context: SqlalchemyContext = attr(default_factory=create_default_context)

    def get_store_meta(self) -> StoreMeta:
        return self.store_meta

    def create(self) -> Optional[StoreABC]:
        table = self.context.get_table(self.store_meta)
        store = SqlalchemyTableStore(self.store_meta, table, self.context.engine)
        store = SchemaValidatingStore(store)
        store = restrict_access_store(store, self.store_meta.store_access)
        store = triggered_store(store)
        return store
