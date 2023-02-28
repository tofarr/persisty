from datetime import datetime
from typing import Optional
from uuid import UUID

from persisty.stored import stored


@stored
class ContentMeta:
    key: str
    stream_id: Optional[UUID]
    content_type: Optional[str]
    etag: str
    size: int
    updated_at: datetime
