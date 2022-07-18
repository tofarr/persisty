from __future__ import annotations
from typing import Tuple

from dataclasses import dataclass

from marshy.types import ExternalItemType

from persisty.access_control.access_control_abc import AccessControlABC
from persisty.access_control.access_control import ALL_ACCESS
from persisty.cache_control.cache_control_abc import CacheControlABC
from persisty.cache_control.secure_hash_cache_control import SecureHashCacheControl
from persisty.key_config.field_key_config import ATTR_KEY_CONFIG
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.storage.field.field import Field


@dataclass(frozen=True)
class StorageMeta:
    name: str
    fields: Tuple[Field, ...] = None
    key_config: KeyConfigABC[ExternalItemType] = ATTR_KEY_CONFIG
    access_control: AccessControlABC[ExternalItemType] = ALL_ACCESS
    cache_control: CacheControlABC[ExternalItemType] = SecureHashCacheControl()
    batch_size: int = 100
