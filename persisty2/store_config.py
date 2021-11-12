from dataclasses import dataclass
from typing import Optional, Generic, TypeVar, Iterable

from persisty2.access_control.access_control import ALL_ACCESS
from persisty2.access_control.access_control_abc import AccessControlABC
from persisty2.attr.attr_abc import AttrABC
from persisty2.cache_control.cache_control_abc import CacheControlABC
from persisty2.cache_control.secure_hash_cache_control import SecureHashCacheControl
from persisty2.key_config.attr_key_config import AttrKeyConfig
from persisty2.key_config.key_config_abc import KeyConfigABC

T = TypeVar('T')


@dataclass(frozen=True)
class StoreConfig(Generic[T]):
    """ Object designed to be externalizable and contain information about a store. """
    name: str
    description: Optional[str] = None

    # Need a way of setting up marshy with types out of the gate. Maybe it needs a subtype registry?
    key_config: KeyConfigABC = AttrKeyConfig()
    access_control: AccessControlABC = ALL_ACCESS
    cache_control: CacheControlABC = SecureHashCacheControl()

    # Maybe this should simply not be, given that the schemas handle this.
    attrs: Optional[Iterable[AttrABC]] = None

    max_page_size: int = 100

