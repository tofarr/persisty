from datetime import datetime
from uuid import UUID

from persisty.index import Index
from persisty.stored import stored


@stored(
    indexes=(Index(('stream_id',)),)
)
class Chunk:
    id: UUID
    item_key: str
    stream_id: UUID
    part_number: int
    data: bytes
    created_at: datetime
    updated_at: datetime
