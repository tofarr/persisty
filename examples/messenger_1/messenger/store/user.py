from dataclasses import field
from datetime import datetime
from typing import Optional, ForwardRef
from uuid import UUID

from schemey.schema import str_schema
from schemey.string_format import StringFormat

from persisty.index.unique_index import UniqueIndex
from persisty.link.has_count import HasCount
from persisty.link.has_many import HasMany
from persisty.attr.attr import Attr
from persisty.result_set import ResultSet
from persisty.stored import stored


@stored(indexes=(UniqueIndex(("username",)),))
class User:
    """Item representing a user object"""

    id: UUID
    username: str = Attr(schema=str_schema(max_length=255))
    full_name: Optional[str] = field(
        default=None, metadata=dict(schemey=str_schema(max_length=255))
    )
    email_address: Optional[str] = field(
        default=None,
        metadata=dict(
            schemey=str_schema(max_length=255, str_format=StringFormat.EMAIL)
        ),
    )
    password_digest: str
    admin: bool = False
    created_at: datetime
    updated_at: datetime
    authored_message_count: int = HasCount(
        linked_store_type=ForwardRef("servey_main.models.message.Message"),
        remote_key_attr_name="author_id",
    )
    authored_messages: ResultSet[
        ForwardRef("servey_main.models.message.Message")
    ] = HasMany(
        remote_key_attr_name="author_id",
    )
