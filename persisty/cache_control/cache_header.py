from dataclasses import dataclass
from datetime import datetime
from email.utils import formatdate
from typing import Optional, Iterator

from persisty.util import filter_none, secure_hash


@dataclass(frozen=True)
class CacheHeader:
    etag: Optional[str] = None
    updated_at: Optional[datetime] = None
    expire_at: Optional[datetime] = None
    private: bool = True

    def get_http_headers(self):
        return filter_none({
            'ETag': self.etag,
            'Cache-Control': self.get_cache_control_str(),
            'Last-Modified': None if self.updated_at is None else formatdate(self.updated_at.timestamp()),
            'Expires': None if self.expire_at is None else formatdate(self.expire_at.timestamp())
        })

    def get_cache_control_str(self):
        if self.expire_at is not None:
            max_age = (self.expire_at - datetime.now()).seconds
            if max_age > 0:
                return f"{'private' if self.private else 'public'},max-age={max_age}"
        return 'no-storage'

    def combine_with(self, cache_headers: Iterator['CacheHeader']) -> 'CacheHeader':
        keys = [self.etag]
        updated_at = self.updated_at
        expire_at = self.expire_at
        for sub_header in cache_headers:
            keys.append(sub_header.etag)
            if updated_at is not None:
                if sub_header.updated_at is None or sub_header.updated_at > updated_at:
                    updated_at = sub_header.updated_at
            if expire_at is not None:
                if sub_header.expire_at is None or sub_header.expire_at < expire_at:
                    expire_at = sub_header.expire_at
        cache_key = secure_hash(keys)
        return CacheHeader(cache_key, updated_at, expire_at)
