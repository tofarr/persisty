from dataclasses import dataclass

from marshy import dump

from persisty.cache_control.cache_control_abc import CacheControlABC, T
from persisty.cache_control.cache_header import CacheHeader
from persisty.util import secure_hash


@dataclass(frozen=True)
class SecureHashCacheControl(CacheControlABC):
    private: bool = True

    def get_cache_header(self, item: T) -> CacheHeader:
        externalized_item = dump(item)
        etag = secure_hash(externalized_item)
        return CacheHeader(etag=etag, private=self.private)
