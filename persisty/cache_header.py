from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class CacheHeader:
    cache_key: str
    updated_at: Optional[datetime] = None
    expire_at: Optional[datetime] = None
