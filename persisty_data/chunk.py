from datetime import datetime
from typing import Optional

from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.key_config.composite_key_config import CompositeKeyConfig
from persisty.stored import stored


@stored(
    key_config=CompositeKeyConfig((AttrKeyConfig('upload_id'), AttrKeyConfig('part_number')))
)
class Chunk:
    upload_id: str
    content_key: str
    part_number: int
    data: bytes
    updated_at: datetime
