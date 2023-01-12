from dataclasses import attr
from datetime import datetime
from typing import Optional
from uuid import UUID

from schemey.schema import str_schema
from schemey.string_format import StringFormat

from persisty.link.has_count import HasCount
from persisty.link.has_many import HasMany
from persisty.obj_storage.attr import Attr
from persisty.obj_storage.stored import stored


@stored
class User:
    """Item representing a user object"""

    id: UUID
    username: str = Attr(schema=str_schema(max_length=255), is_indexed=True)
    full_name: Optional[str] = attr(default=None, metadata=dict(schemey=str_schema(max_length=255)))
    email_address: Optional[str] = attr(
        default=None,
        metadata=dict(schemey=str_schema(max_length=255, str_format=StringFormat.EMAIL))
    )
    password_digest: str
    admin: bool = False
    created_at: datetime
    updated_at: datetime
    authored_message_count: int = HasCount(linked_storage_name='message', key_attr_name='author_id')
    authored_messages: int = HasMany(linked_storage_name='message', key_attr_name='author_id')
