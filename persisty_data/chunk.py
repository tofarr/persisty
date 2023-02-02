from datetime import datetime
from typing import Optional

from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.key_config.composite_key_config import CompositeKeyConfig
from persisty.stored import stored


@stored(
    key_config=CompositeKeyConfig((AttrKeyConfig('content_key'), AttrKeyConfig('part_number')))
)
class Chunk:
    content_key: str
    part_number: int
    upload_id: Optional[str]
    data: bytes
    created_at: datetime
    updated_at: datetime
