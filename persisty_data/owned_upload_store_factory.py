from dataclasses import dataclass
from typing import Optional

from servey.security.authorization import Authorization

from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.store.store_abc import StoreABC
from persisty.store_meta import T, StoreMeta
from persisty_data.owned_upload_store import OwnedUploadStore
from persisty_data.upload import Upload


@dataclass
class OwnedUploadStoreFactory(StoreFactoryABC[Upload]):
    store_factory: StoreFactoryABC[Upload]

    def get_meta(self) -> StoreMeta:
        return self.store_factory.get_meta()

    def create(self, authorization: Optional[Authorization]) -> Optional[StoreABC[T]]:
        store = self.store_factory.create(authorization)
        store = OwnedUploadStore(store, authorization)
        return store
