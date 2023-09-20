from dataclasses import dataclass, field
from typing import Optional

from marshy.types import ExternalItemType

from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.impl.mem.mem_store import MemStore
from persisty.store.referential_integrity_store import ReferentialIntegrityStore
from persisty.store.schema_validating_store import SchemaValidatingStore
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta
from persisty.trigger.wrapper import triggered_store


@dataclass
class MemStoreFactory(StoreFactoryABC):
    items: ExternalItemType = field(default_factory=dict)
    referential_integrity: bool = False
    _cached_store: Optional[StoreABC] = None

    def create(self, store_meta: StoreMeta) -> Optional[StoreABC]:
        store = self._cached_store
        if not store:
            store = MemStore(store_meta, self.items)
            store = SchemaValidatingStore(store)
            store = store_meta.store_security.get_unsecured(store)
            store = triggered_store(store)
            if self.referential_integrity:
                store = ReferentialIntegrityStore(store)
            self._cached_store = store
        return store
