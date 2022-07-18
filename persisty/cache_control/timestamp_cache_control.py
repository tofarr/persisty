from dataclasses import dataclass

from marshy.types import ExternalItemType

from persisty.cache_control.cache_control_abc import CacheControlABC
from persisty.cache_control.cache_header import CacheHeader
from persisty.cache_control.secure_hash_cache_control import SecureHashCacheControl


@dataclass(frozen=True)
class TimestampCacheControl(CacheControlABC[ExternalItemType]):
    cache_control: CacheControlABC = SecureHashCacheControl()
    updated_at_attr: str = 'updated_at'

    def get_cache_header(self, item: ExternalItemType):
        cache_header = self.cache_control.get_cache_header(item)
        updated_at = item.get(self.updated_at_attr)
        return CacheHeader(etag=cache_header.etag,
                           updated_at=updated_at,
                           expire_at=cache_header.expire_at,
                           private=cache_header.private)
