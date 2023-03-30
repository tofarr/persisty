from dataclasses import dataclass, field
from typing import Optional

from persisty.impl.sqlalchemy.sqlalchemy_context import SqlalchemyContext
from persisty.impl.sqlalchemy.sqlalchemy_context_factory_abc import (
    get_default_context,
)
from persisty.impl.sqlalchemy.sqlalchemy_table_store import SqlalchemyTableStore
from persisty.store.restrict_access_store import restrict_access_store
from persisty.store.schema_validating_store import SchemaValidatingStore
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta
from persisty.trigger.wrapper import triggered_store


@dataclass
class SqlalchemyTableStoreFactory:
    store_meta: StoreMeta
    context: SqlalchemyContext = field(default_factory=get_default_context)

    def get_meta(self) -> StoreMeta:
        return self.store_meta

    def create(self) -> Optional[StoreABC]:
        table = self.context.get_table(self.store_meta)
        store = SqlalchemyTableStore(self.store_meta, table, self.context.engine)
        store = SchemaValidatingStore(store)
        store = restrict_access_store(store, self.store_meta.store_access)
        store = triggered_store(store)
        return store
