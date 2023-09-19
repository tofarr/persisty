from dataclasses import dataclass, field
from typing import Dict

from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta


@dataclass
class CachingStoreFactory(StoreFactoryABC):
    factory: StoreFactoryABC
    cache: Dict[str, StoreABC] = field(default_factory=dict)

    def create(self, meta: StoreMeta) -> StoreABC:
        store = self.cache.get(meta.name)
        if not store:
            store = self.factory.create(meta)
            self.cache[meta.name] = store
        return store
