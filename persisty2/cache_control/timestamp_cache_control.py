from dataclasses import dataclass

from persisty2.cache_control.cache_control_abc import T
from persisty2.cache_control.cache_header import CacheHeader
from persisty2.cache_control.secure_hash_cache_control import SecureHashCacheControl


@dataclass(frozen=True)
class TimestampCacheControl(SecureHashCacheControl[T]):
    updated_at_attr: str = 'updated_at'

    def get_cache_header(self, item: T):
        cache_header = super().get_cache_header(item)
        updated_at = getattr(item, self.updated_at_attr)
        return CacheHeader(etag=cache_header.etag, updated_at=updated_at, private=self.private)
