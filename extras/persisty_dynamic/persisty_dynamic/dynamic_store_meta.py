from typing import Optional, Tuple

from servey.cache_control.cache_control_abc import CacheControlABC
from servey.cache_control.secure_hash_cache_control import SecureHashCacheControl

from persisty.attr.attr import Attr
from persisty.index import Index
from persisty.key_config.attr_key_config import ATTR_KEY_CONFIG, AttrKeyConfig
from persisty.store_meta import StoreMeta
from persisty.stored import stored
from persisty_dynamic.dynamic_security_model import DynamicSecurityModel


@stored
class DynamicStoreMeta:
    name: str
    attrs: Tuple[Attr, ...]
    security_model: DynamicSecurityModel
    key_config: AttrKeyConfig = ATTR_KEY_CONFIG
    cache_control: CacheControlABC = SecureHashCacheControl()
    batch_size: int = 100
    description: Optional[str] = None
    indexes: Tuple[Index, ...] = tuple()

    def to_meta(self) -> StoreMeta:
        pass
