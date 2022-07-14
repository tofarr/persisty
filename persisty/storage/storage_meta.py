from typing import TypeVar, Type
from dataclasses import dataclass

from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.security.access_control import ALL_ACCESS
from persisty.security.access_control_abc import AccessControlABC
from persisty.cache_control.cache_control_abc import CacheControlABC
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order_abc import SearchOrderABC

T = TypeVar('T')
F = TypeVar('F', bound=SearchFilterABC)
C = TypeVar('C', bound=SearchOrderABC)


@dataclass(frozen=True)
class StorageMeta:
    name: str
    item_type: Type[T]
    search_filter_type: Type[F]
    search_order_type: Type[C]
    cache_control: CacheControlABC[T]
    key_config: KeyConfigABC[T]
    access_control: AccessControlABC = ALL_ACCESS
    batch_size: int = 100
