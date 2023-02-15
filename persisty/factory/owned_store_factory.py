from dataclasses import dataclass
from typing import Optional

from servey.security.authorization import Authorization

from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.store.owned_store import OwnedStore
from persisty.store.store_abc import StoreABC
from persisty.store_meta import T, StoreMeta


@dataclass
class OwnedStoreFactory(StoreFactoryABC[T]):
    store_factory: StoreFactoryABC[T]
    attr_name: str

    def get_meta(self) -> StoreMeta:
        return self.store_factory.get_meta()

    def create(self, authorization: Optional[Authorization]) -> Optional[StoreABC[T]]:
        store = self.store_factory.create(authorization)
        store = OwnedStore(store, authorization, self.attr_name)
        return store
