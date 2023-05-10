from datetime import datetime
from uuid import UUID

from persisty.attr.attr import Attr
from persisty.impl.dynamodb.partition_sort_index import PartitionSortIndex
from persisty.index.attr_index import AttrIndex
from persisty.stored import stored
from schemey.schema import int_schema, str_schema


@stored(
    indexes=(AttrIndex("stream_id"), PartitionSortIndex("stream_id", "part_number"))
)
class Chunk:
    id: UUID
    item_key: str = Attr(updatable=False, schema=str_schema(max_length=255))
    stream_id: UUID = Attr(updatable=False)
    part_number: int = Attr(updatable=False, schema=int_schema(minimum=1))
    data: bytes
    created_at: datetime
    updated_at: datetime
