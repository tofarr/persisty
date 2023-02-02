from datetime import datetime
from typing import List, Optional

from persisty.stored import stored


@stored
class ContentMeta:
    key: str
    upload_id: str
    content_type: Optional[str]
    etag: str
    size_in_bytes: int
    created_at: datetime
    updated_at: datetime
