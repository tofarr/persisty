from typing import TypeVar, Type, Tuple
from dataclasses import dataclass

from persisty.item.field import Field, fields_for_type
from persisty.key_config.attr_key_config import ATTR_KEY_CONFIG, AttrKeyConfig
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
    fields: Tuple[Field] = None
    key_config: KeyConfigABC[T] = ATTR_KEY_CONFIG
    access_control: AccessControlABC = ALL_ACCESS
    batch_size: int = 100

    def __post_init__(self):
        if self.fields is None:
            fields = fields_for_type(self.item_type)
            object.__setattr__(self, 'fields', fields)
        if isinstance(self.key_config, AttrKeyConfig):
            id_field = next((f for f in self.fields if f.name == self.key_config.id_attr_name), None)
            if not id_field:
                raise ValueError('no_field_for_key')
