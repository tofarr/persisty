from dataclasses import field
from uuid import UUID

from schemey.schema import str_schema
from schemey.string_format import StringFormat

from persisty.access_control.constants import ALL_ACCESS
from persisty.access_control.factory.default_access_control_factory import (
    DefaultAccessControlFactory,
)
from persisty.impl.default_storage_factory import DefaultStorageFactory
from persisty.impl.mem.mem_storage_factory import MemStorageFactory
from persisty.impl.sqlalchemy.sqlalchemy_table_storage_factory import (
    SqlalchemyTableStorageFactory,
)
from persisty.link.belongs_to import BelongsTo
from persisty.link.has_count import HasCount
from persisty.link.has_many import HasMany
from persisty.obj_storage.stored import stored, get_storage_meta
from persisty.storage.result_set import ResultSet


@stored(access_control_factories=(DefaultAccessControlFactory(ALL_ACCESS),))
class User:
    """Item representing a user object"""

    id: UUID
    full_name: str = field(metadata=dict(schemey=str_schema(max_length=255)))
    email_address: str = field(
        metadata=dict(schemey=str_schema(max_length=255, str_format=StringFormat.EMAIL))
    )
    message_count: int = HasCount()
    message_result_set: ResultSet["Message"] = HasMany()


@stored(access_control_factories=(DefaultAccessControlFactory(ALL_ACCESS),))
class Message:
    """Item representing a message object"""

    id: UUID
    text: str
    user: User = BelongsTo()


# user_storage_factory = MemStorageFactory(get_storage_meta(User))
# message_storage_factory = MemStorageFactory(get_storage_meta(Message))
# user_storage_factory = SqlalchemyTableStorageFactory(get_storage_meta(User))
# message_storage_factory = SqlalchemyTableStorageFactory(get_storage_meta(Message))
user_storage_factory = DefaultStorageFactory(get_storage_meta(User))
message_storage_factory = DefaultStorageFactory(get_storage_meta(Message))
