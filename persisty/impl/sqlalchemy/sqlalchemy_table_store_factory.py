from dataclasses import dataclass, field
from typing import Optional

from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.impl.sqlalchemy.sqlalchemy_context import SqlalchemyContext
from persisty.impl.sqlalchemy.sqlalchemy_context_factory_abc import (
    get_default_context,
)
from persisty.impl.sqlalchemy.sqlalchemy_table_store import SqlalchemyTableStore
from persisty.store.referential_integrity_store import ReferentialIntegrityStore
from persisty.store.schema_validating_store import SchemaValidatingStore
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta
from persisty.trigger.wrapper import triggered_store


@dataclass
class SqlalchemyTableStoreFactory(StoreFactoryABC):
    context: SqlalchemyContext = field(default_factory=get_default_context)
    # Lack of referential integrity may be acceptable, or this may be handled by the db engine
    referential_integrity: bool = False

    def create(self, store_meta: StoreMeta) -> Optional[StoreABC]:
        table = self.context.get_table(store_meta)
        store = SqlalchemyTableStore(store_meta, table, self.context.engine)
        store = SchemaValidatingStore(store)
        store = triggered_store(store)
        if self.referential_integrity:
            store = ReferentialIntegrityStore(store)
        return store
