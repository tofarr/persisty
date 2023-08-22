from dataclasses import dataclass
from typing import Optional

from servey.security.authorization import Authorization

from persisty.factory.store_factory_abc import StoreFactoryABC, T
from persisty.store.linked_store import LinkedStore
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta


@dataclass
class LinkedStoreFactory(StoreFactoryABC[T]):
    factory: StoreFactoryABC[T]

    def get_meta(self) -> StoreMeta:
        return self.factory.get_meta()

    def create(self, authorization: Optional[Authorization]) -> Optional[StoreABC[T]]:
        return LinkedStore(self.factory.create(authorization), authorization)
