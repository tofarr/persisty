"""
Classes for treating MetaStorage as an object storage
"""
from dataclasses import dataclass
from enum import Enum
from typing import Union

from marshy import get_default_context

from persisty.context.meta_storage_abc import STORED_STORAGE_META
from persisty.key_config.field_key_config import FieldKeyConfig
from persisty.obj_storage.obj_storage import ObjStorage
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


META_KEY_CONFIG = FieldKeyConfig("name")
StorageMetaCreateInput = StorageMeta
StorageMetaUpdateInput = with_undefined_state(StorageMeta, "StorageMetaUpdateInput")
STORAGE_META_MARSHALLER = get_default_context().get_marshaller(StorageMeta)
OBJ_STORAGE_META = ObjStorageMeta(
    storage_meta=STORED_STORAGE_META,
    item_type=StorageMeta,
    search_filter_factory_type=StorageMetaSearchFilter,
    search_order_factory_type=StorageMetaSearchOrder,
    create_input_type=StorageMetaCreateInput,
    update_input_type=StorageMetaUpdateInput,
)
ObjMetaStorage = ObjStorage[
    StorageMeta,
    StorageMetaSearchFilter,
    StorageMetaSearchOrder,
    StorageMetaCreateInput,
    StorageMetaUpdateInput,
]


def obj_meta_storage(storage: StorageABC) -> ObjMetaStorage:
    return ObjStorage(storage, OBJ_STORAGE_META)
