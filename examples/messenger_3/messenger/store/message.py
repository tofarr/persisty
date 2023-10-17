from datetime import datetime
from typing import ForwardRef
from uuid import UUID

from persisty.link.belongs_to import BelongsTo
from persisty.security.owned_store_security import OwnedStoreSecurity
from persisty.stored import stored


@stored(store_security=OwnedStoreSecurity(subject_id_attr_name="author_id"))
class Message:
    """Item representing a message object"""

    id: UUID
    message_text: str
    created_at: datetime
    updated_at: datetime
    author_id: UUID
    author: ForwardRef("servey_main.models.user.User") = BelongsTo()
