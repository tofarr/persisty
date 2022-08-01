from uuid import UUID

from marshy.factory.dataclass_marshaller_factory import DataclassMarshallerFactory

from persisty.access_control.factory.access_control_factory import AccessControlFactory
from persisty.access_control.factory.permission_access_control_factory import (
    PermissionAccessControlFactory,
)
from persisty.context import PersistyContext
from persisty.impl.mem.mem_meta_storage import MemMetaStorage
from persisty.util import UNDEFINED, get_logger

priority = 100


def create_meta_storage():
    """The default is MemMetaStorage. This can be overridden to provide Dynamodb, SQL, or other composite options."""
    # TODO: A Sqlite / Sqlalchemy default would be interesting
    return MemMetaStorage()


def configure_context(persisty_context: PersistyContext):
    persisty_context.register_access_control_factory(AccessControlFactory())
    # By default, root can do anything
    persisty_context.register_access_control_factory(
        PermissionAccessControlFactory("root")
    )

    # We do this here to affect only the marshallers associated with schemey
    persisty_context.schema_context.marshaller_context.register_factory(
        DataclassMarshallerFactory(101, (UNDEFINED,))
    )
