from dataclasses import dataclass
from typing import Optional, Iterator

from persisty.factory.store_factory_abc import ROUTE
from persisty.store.store_abc import StoreABC
from persisty.store_meta import T, StoreMeta
from servey.action.action import Action
from servey.security.authorization import Authorization

from persisty_data.data_store_factory_abc import DataStoreFactoryABC
from persisty_data.owned_data_store import OwnedDataStore
from persisty_data.upload_form import UploadForm


@dataclass
class OwnedDataStoreFactory(DataStoreFactoryABC):
    data_store_factory: DataStoreFactoryABC
    require_owner_for_read: bool = False
    require_owner_for_update: bool = True
    require_owner_for_delete: bool = True

    def get_meta(self) -> StoreMeta:
        return self.data_store_factory.get_meta()

    def create(self, authorization: Optional[Authorization]) -> Optional[StoreABC[T]]:
        store = OwnedDataStore(
            store=self.data_store_factory.create(authorization),
            authorization=authorization,
            require_owner_for_read=self.require_owner_for_read,
            require_owner_for_update=self.require_owner_for_update,
            require_owner_for_delete=self.require_owner_for_delete
        )
        return store

    def get_upload_form(self, key: str, authorization: Optional[Authorization]) -> UploadForm:
        return self.data_store_factory.get_upload_form(key, authorization)

    def get_download_url(self, key: str, authorization: Optional[Authorization]) -> str:
        return self.data_store_factory.get_download_url(key, authorization)

    def create_routes(self) -> Iterator[ROUTE]:
        return self.data_store_factory.create_routes()

    def create_actions(self) -> Iterator[Action]:
        return self.data_store_factory.create_actions()
