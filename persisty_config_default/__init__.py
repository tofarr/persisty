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
    persisty_context.schema_context.marshaller_context.register_factory(DataclassMarshallerFactory(101, (UNDEFINED,)))

    # TODO: Delete me! This is so wrong - it will probably fail on non mem databases anyway
    from tests.fixtures.number_name import NumberName
    from persisty.obj_storage.stored import get_storage_meta
    from marshy import dump
    from persisty.storage.batch_edit import Create
    from persisty.access_control.authorization import ROOT

    storage_meta = get_storage_meta(NumberName)
    persisty_context.get_meta_storage(ROOT).create(dump(storage_meta))

    storage = persisty_context.get_storage(storage_meta.name, ROOT)
    results = storage.edit_batch(
        [
            Create(dict(id="00000000-0000-0000-0000-000000000" + (str(1000 + v)[1:]), title=t, value=v))
            for v, t in enumerate(("Zero", "One", "Two", "Three", "Four", "Five"))
        ]
    )
    get_logger(__name__).info(f"{sum(1 for r in results if r.success)} items created...")
