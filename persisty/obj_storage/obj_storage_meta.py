from __future__ import annotations
from typing import Generic, TypeVar, Type

from dataclasses import dataclass

from persisty.access_control.access_control_abc import AccessControlABC
from persisty.access_control.access_control import ALL_ACCESS
from persisty.cache_control.cache_control_abc import CacheControlABC
from persisty.cache_control.secure_hash_cache_control import SecureHashCacheControl
from persisty.key_config.field_key_config import ATTR_KEY_CONFIG
from persisty.key_config.obj_key_config_abc import ObjKeyConfigABC
from persisty.obj_storage.search_filter_factory.search_filter_factory_abc import SearchFilterFactoryABC
from persisty.obj_storage.search_order.search_order_factory_abc import SearchOrderFactoryABC

T = TypeVar('T')
F = TypeVar('F', bound=SearchFilterFactoryABC)
S = TypeVar('S', bound=SearchOrderFactoryABC)
C = TypeVar('C')
U = TypeVar('U')


@dataclass(frozen=True)
class ObjStorageMeta(Generic[T]):
    """ Storage meta for object storage """
    name: str
    item_type: Type[T]
    search_filter_factory_type: Type[F]
    search_order_factory_type: Type[S]
    create_input_type: Type[C]
    update_input_type: Type[U]
    key_config: ObjKeyConfigABC[T] = ATTR_KEY_CONFIG
    access_control: AccessControlABC[T] = ALL_ACCESS
    cache_control: CacheControlABC[T] = SecureHashCacheControl()
    batch_size: int = 100