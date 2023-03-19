from dataclasses import dataclass
from typing import Optional

from servey.security.authorization import Authorization

from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.store.store_abc import StoreABC
from persisty.store_meta import T, StoreMeta


@dataclass
class DefaultStoreFactory(StoreFactoryABC[T]):
    """
    No op store factory which doesn't actually use authorization
    """

    store: StoreABC

    def get_meta(self) -> StoreMeta:
        return self.store.get_meta()

    def create(self, authorization: Optional[Authorization]) -> Optional[StoreABC[T]]:
        return self.store
