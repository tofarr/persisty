from dataclasses import dataclass

from persisty.cache_control.cache_control_abc import T, CacheControlABC
from persisty.cache_control.cache_header import CacheHeader
from persisty.cache_control.secure_hash_cache_control import SecureHashCacheControl


@dataclass(frozen=True)
class TimestampCacheControl(CacheControlABC[T]):
    cache_control: CacheControlABC = SecureHashCacheControl()
    updated_at_attr: str = 'updated_at'

    def get_cache_header(self, item: T):
        cache_header = self.cache_control.get_cache_header(item)
        updated_at = getattr(item, self.updated_at_attr)
        return CacheHeader(etag=cache_header.etag,
                           updated_at=updated_at,
                           expire_at=cache_header.expire_at,
                           private=cache_header.private)
