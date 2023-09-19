from dataclasses import dataclass, field
from typing import Optional

from persisty.impl.sqlalchemy.sqlalchemy_context import SqlalchemyContext
from persisty.impl.sqlalchemy.sqlalchemy_context_factory_abc import (
    get_default_context,
)
from persisty.impl.sqlalchemy.sqlalchemy_table_store import SqlalchemyTableStore
from persisty.security.restrict_access_store import restrict_access_store
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
        store_meta = self.store_meta
        table = self.context.get_table(store_meta)
        store = SqlalchemyTableStore(store_meta, table, self.context.engine)
        store = SchemaValidatingStore(store)
        store = store_meta.store_security.get_unsecured(store)
        store = triggered_store(store)
        return store
