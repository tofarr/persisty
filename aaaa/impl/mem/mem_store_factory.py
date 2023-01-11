from dataclasses import dataclass, field
from typing import Optional

from marshy.types import ExternalItemType

from aaaa.impl.mem.mem_store import MemStore
from aaaa.store.restrict_access_store import restrict_access_store
from aaaa.store.schema_validating_store import SchemaValidatingStore
from aaaa.store.store_abc import StoreABC
from aaaa.store.store_factory_abc import StoreFactoryABC
from aaaa.store_meta import StoreMeta
from aaaa.trigger.wrapper import triggered_store


@dataclass
class MemStoreFactory(StoreFactoryABC):
    store_meta: StoreMeta
    items: ExternalItemType = field(default_factory=dict)

    def get_meta(self) -> StoreMeta:
        return self.store_meta

    def create(self) -> Optional[StoreABC]:
        store = MemStore(self.store_meta, self.items)
        store = SchemaValidatingStore(store)
        store = restrict_access_store(store, self.store_meta.store_access)
        store = triggered_store(store)
        return store
