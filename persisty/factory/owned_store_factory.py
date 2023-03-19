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
    subject_id_attr_name: str = "subject_id"
    require_ownership_for_read: bool = False
    require_ownership_for_update: bool = True
    require_ownership_for_delete: bool = True

    def get_meta(self) -> StoreMeta:
        return self.store_factory.get_meta()

    def create(self, authorization: Optional[Authorization]) -> Optional[StoreABC[T]]:
        store = self.store_factory.create(authorization)
        store = OwnedStore(
            store=store,
            authorization=authorization,
            subject_id_attr_name=self.subject_id_attr_name,
            require_ownership_for_read=self.require_ownership_for_read,
            require_ownership_for_delete=self.require_ownership_for_delete,
            require_ownership_for_update=self.require_ownership_for_update,
        )
        return store
