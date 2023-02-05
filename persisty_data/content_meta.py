from datetime import datetime
from typing import Optional

from servey.cache_control.secure_hash_cache_control import SecureHashCacheControl
from servey.cache_control.ttl_cache_control import TtlCacheControl

from persisty.stored import stored


@stored(
    cache_control=TtlCacheControl(3600, SecureHashCacheControl(), True)
)
class ContentMeta:
    key: str
    upload_id: str
    content_type: Optional[str]
    etag: str
    size_in_bytes: int
    created_at: datetime
    updated_at: datetime
