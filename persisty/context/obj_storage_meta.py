from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Type, Union

from marshy import get_default_context

from persisty.access_control.obj_access_control_abc import ObjAccessControlABC
from persisty.cache_control.obj_cache_control import ObjCacheControl
from persisty.cache_control.secure_hash_cache_control import SecureHashCacheControl
from persisty.key_config.field_key_config import FieldKeyConfig
from persisty.obj_storage.obj_storage_abc import ObjStorageABC
from persisty.obj_storage.obj_storage_meta import ObjStorageMeta
from persisty.search_filter.search_filter_factory_abc import SearchFilterFactoryABC
from persisty.search_order.search_order_factory_abc import SearchOrderFactoryABC
from persisty.obj_storage.with_undefined_state import with_undefined_state
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta
from persisty.util.undefined import UNDEFINED, Undefined


@dataclass
class StorageMetaSearchFilter(SearchFilterFactoryABC):
    query: Union[str, Undefined] = UNDEFINED


class StorageMetaSearchOrderField(Enum):
    NAME = "name"
    # Future search fields to be added here...


@dataclass
class StorageMetaSearchOrder(SearchOrderFactoryABC):
    field: StorageMetaSearchOrderField = StorageMetaSearchOrderField.NAME
    desc: bool = False

    @staticmethod
    def key(storage: Union[StorageABC, StorageMeta]) -> str:
        if isinstance(storage, StorageABC):
            storage = storage.storage_meta
        return storage.name


META_KEY_CONFIG = FieldKeyConfig("name")
CreateStorageMetaInput = with_undefined_state(StorageMeta, "CreateStorageMetaInput")
UpdateStorageMetaInput = with_undefined_state(StorageMeta, "UpdateStorageMetaInput")
STORAGE_META_MARSHALLER = get_default_context().get_marshaller(StorageMeta)


class MetaStorageABC(
    ObjStorageABC[
        StorageMeta,
        StorageMetaSearchFilter,
        StorageMetaSearchOrder,
        CreateStorageMetaInput,
        UpdateStorageMetaInput,
    ],
    ABC,
):
    @property
    @abstractmethod
    def access_control(self) -> ObjAccessControlABC[StorageMeta]:
        """Get the access control for this storage"""

    @property
    def obj_storage_meta(
        self,
    ) -> ObjStorageMeta[
        StorageMeta,
        StorageMetaSearchFilter,
        StorageMetaSearchOrder,
        CreateStorageMetaInput,
        UpdateStorageMetaInput,
    ]:
        return ObjStorageMeta(
            name="meta",
            item_type=StorageMeta,
            search_filter_factory_type=StorageMetaSearchFilter,
            search_order_factory_type=StorageMetaSearchOrder,
            create_input_type=CreateStorageMetaInput,
            update_input_type=UpdateStorageMetaInput,
            key_config=META_KEY_CONFIG,
            access_control=self.access_control,
            cache_control=ObjCacheControl(
                SecureHashCacheControl(), STORAGE_META_MARSHALLER
            ),
            batch_size=100,
        )

    @property
    def item_type(self) -> Type[StorageMeta]:
        return StorageMeta

    @property
    def search_filter_factory_type(self) -> Type[StorageMetaSearchFilter]:
        return StorageMetaSearchFilter

    @property
    def search_order_factory_type(self) -> Type[StorageMetaSearchOrder]:
        return StorageMetaSearchOrder

    @property
    def create_input_type(self) -> Type[CreateStorageMetaInput]:
        return CreateStorageMetaInput

    @property
    def update_input_type(self) -> Type[UpdateStorageMetaInput]:
        return UpdateStorageMetaInput
