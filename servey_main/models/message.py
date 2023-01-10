from datetime import datetime
from typing import ForwardRef
from uuid import UUID

from persisty.link.belongs_to import BelongsTo
from persisty.obj_storage.stored import stored


@stored
class Message:
    """Item representing a message object"""
    id: UUID
    text: str
    created_at: datetime
    updated_at: datetime
    author: ForwardRef('servey_main.models.User') = BelongsTo()
    # recipient: Optional[User] = BelongsTo()