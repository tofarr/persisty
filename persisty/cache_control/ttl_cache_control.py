from dataclasses import dataclass
from datetime import datetime
from time import time

from marshy.types import ExternalItemType

from persisty.cache_control.cache_control_abc import CacheControlABC
from persisty.cache_control.cache_header import CacheHeader
from persisty.cache_control.secure_hash_cache_control import SecureHashCacheControl


@dataclass(frozen=True)
class TTLCacheControl(CacheControlABC):
    ttl: int
    cache_control: CacheControlABC = SecureHashCacheControl()

    def get_cache_header(self, item: ExternalItemType):
        cache_header = self.cache_control.get_cache_header(item)
        expire_at = datetime.fromtimestamp(int(time()) + self.ttl)
        return CacheHeader(
            etag=cache_header.etag,
            updated_at=cache_header.updated_at,
            expire_at=expire_at,
            private=cache_header.private,
        )
