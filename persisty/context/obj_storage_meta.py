from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import Type, Union

from persisty.obj_storage.obj_storage_abc import ObjStorageABC
from persisty.obj_storage.search_filter_factory.search_filter_factory_abc import SearchFilterFactoryABC
from persisty.obj_storage.search_order.search_order_factory_abc import SearchOrderFactoryABC
from persisty.obj_storage.with_undefined_state import with_undefined_state
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta
from persisty.util.undefined import UNDEFINED, Undefined


@dataclass
class StorageMetaSearchFilter(SearchFilterFactoryABC):
    query: Union[str, Undefined] = UNDEFINED


class StorageMetaSearchOrderField(Enum):
    NAME = 'name'
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


CreateStorageMetaInput = with_undefined_state(StorageMeta, 'CreateStorageMetaInput')
UpdateStorageMetaInput = with_undefined_state(StorageMeta, 'UpdateStorageMetaInput')


class MetaStorageABC(ObjStorageABC[StorageMeta, StorageMetaSearchFilter, StorageMetaSearchOrder, CreateStorageMetaInput,
                                   UpdateStorageMetaInput], ABC):

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
