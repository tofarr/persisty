from dataclasses import dataclass
from datetime import datetime
from email.utils import formatdate
from typing import Optional, Iterator

from persisty.util import filter_none, secure_hash


@dataclass(frozen=True)
class CacheHeader:
    cache_key: str
    updated_at: Optional[datetime] = None
    expire_at: Optional[datetime] = None

    def get_cache_control_headers(self):
        return filter_none({
            'ETag': self.cache_key,
            'Cache-Control': self.get_cache_control_str(),
            'Last-Modified': None if self.updated_at is None else formatdate(self.updated_at)
        })

    def get_cache_control_str(self):
        if self.expire_at is not None:
            max_age = (self.expire_at - datetime.now()).seconds
            if max_age > 0:
                return f'private,max-age={max_age}'
        return 'no-store'

    def combine_with(self, cache_headers: Iterator['CacheHeader']) -> 'CacheHeader':
        keys = [self.cache_key]
        updated_at = self.updated_at
        expire_at = self.expire_at
        for sub_header in cache_headers:
            keys.append(sub_header.cache_key)
            if updated_at is not None:
                if sub_header.updated_at is None or sub_header.updated_at > updated_at:
                    updated_at = sub_header.updated_at
            if expire_at is not None:
                if sub_header.expire_at is None or sub_header.expire_at < expire_at:
                    expire_at = sub_header.expire_at
        cache_key = secure_hash(keys)
        return CacheHeader(cache_key, updated_at, expire_at)
