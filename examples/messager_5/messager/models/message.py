from datetime import datetime
from typing import ForwardRef, Optional
from uuid import UUID

from persisty.impl.dynamodb.partition_sort_index import PartitionSortIndex
from persisty.link.belongs_to import BelongsTo
from persisty.stored import stored

from messager.models.message_state import MessageState


@stored(
    indexes=(
        PartitionSortIndex('author_id', 'created_at'),
        PartitionSortIndex('message_state', 'created_at'),
    )

)
class Message:
    """Item representing a message object"""

    id: UUID
    message_state: Optional[MessageState] = MessageState.FEATURED
    message_text: str
    created_at: datetime
    updated_at: datetime
    author_id: UUID
    author = BelongsTo(linked_store_type=ForwardRef("servey_main.models.user.User"))
