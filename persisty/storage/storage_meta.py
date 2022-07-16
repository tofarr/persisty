from typing import Tuple

from persisty.storage.access_control.access_control_abc import AccessControlABC
from persisty.storage.access_control.access_control import ALL_ACCESS
from persisty.storage.cache_control.cache_control_abc import CacheControlABC
from persisty.storage.cache_control.secure_hash_cache_control import SecureHashCacheControl
from persisty.storage.field.field import Field
from persisty.storage.key_config.field_key_config import ATTR_KEY_CONFIG
from persisty.storage.key_config.key_config_abc import KeyConfigABC


class StorageMeta:
    name: str
    fields: Tuple[Field, ...] = None
    key_config: KeyConfigABC = ATTR_KEY_CONFIG
    access_control: AccessControlABC = ALL_ACCESS
    cache_control: CacheControlABC = SecureHashCacheControl()
    batch_size: int = 100
