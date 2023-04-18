from datetime import datetime
from typing import ForwardRef
from uuid import UUID

from persisty.impl.dynamodb.partition_sort_index import PartitionSortIndex
from persisty.link.belongs_to import BelongsTo
from persisty.stored import stored


@stored(
    indexes=(PartitionSortIndex(('author_id', 'created_at')),)
)
class Message:
    """Item representing a message object"""

    id: UUID
    message_text: str
    created_at: datetime
    updated_at: datetime
    author_id: UUID
    author = BelongsTo(linked_store_type=ForwardRef("servey_main.models.user.User"))
