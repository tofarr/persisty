from dataclasses import field
from uuid import UUID

from schemey.schema import str_schema
from schemey.string_format import StringFormat

from persisty.access_control.constants import ALL_ACCESS
from persisty.access_control.factory.default_access_control_factory import (
    DefaultAccessControlFactory,
)
from persisty.impl.mem.mem_storage_factory import MemStorageFactory
from persisty.obj_storage.stored import stored, get_storage_meta


@stored(access_control_factories=(DefaultAccessControlFactory(ALL_ACCESS),))
class User:
    """Item representing a user object"""

    id: UUID
    full_name: str = field(metadata=dict(schemey=str_schema(max_length=255)))
    email_address: str = field(
        metadata=dict(schemey=str_schema(max_length=255, str_format=StringFormat.EMAIL))
    )


user_storage_factory = MemStorageFactory(get_storage_meta(User))

