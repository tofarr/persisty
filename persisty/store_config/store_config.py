from dataclasses import dataclass
from typing import Optional, Generic, TypeVar, List

from schemey.schema_abc import SchemaABC

from persisty.store_config.access_control.access_control import ALL_ACCESS
from persisty.store_config.access_control.access_control_abc import AccessControlABC
from persisty.store_config.cache_control.cache_control_abc import CacheControlABC
from persisty.store_config.cache_control.secure_hash_cache_control import SecureHashCacheControl
from persisty.store_config.key_config.attr_key_config import AttrKeyConfig
from persisty.store_config.key_config.key_config_abc import KeyConfigABC

T = TypeVar('T')


@dataclass(frozen=True)
class StoreConfig(Generic[T]):
    name: str
    attrs: List[AttrABC]

    description: Optional[str] = None

    # Need a way of setting up marshy with types out of the gate. Maybe it needs a subtype registry?
    key_config: KeyConfigABC = AttrKeyConfig()
    access_control: AccessControlABC = ALL_ACCESS
    cache_control: CacheControlABC = SecureHashCacheControl()

    max_page_size: int = 100
