from dataclasses import field
from uuid import UUID

from schemey.schema import str_schema
from schemey.string_format import StringFormat

from persisty.access_control.constants import ALL_ACCESS
from persisty.access_control.factory.default_access_control_factory import (
    DefaultAccessControlFactory,
)
from persisty.impl.mem.mem_storage_factory import MemStorageFactory
from persisty.link.belongs_to import BelongsTo
from persisty.link.has_count import HasCount
from persisty.link.has_many import HasMany
from persisty.obj_storage.stored import stored, get_storage_meta


@stored(access_control_factories=(DefaultAccessControlFactory(ALL_ACCESS),))
class User:
    """Item representing a user object"""

    id: UUID
    full_name: str = field(metadata=dict(schemey=str_schema(max_length=255)))
    email_address: str = field(
        metadata=dict(schemey=str_schema(max_length=255, str_format=StringFormat.EMAIL))
    )
    #message_count: int = HasCount()
    message_count = HasCount()
    message_result_set = HasMany()


@stored(access_control_factories=(DefaultAccessControlFactory(ALL_ACCESS),))
class Message:
    """Item representing a user object"""

    id: UUID
    text: str
    user: User = BelongsTo()


user_storage_factory = MemStorageFactory(get_storage_meta(User))
message_storage_factory = MemStorageFactory(get_storage_meta(Message))
