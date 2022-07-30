from persisty.access_control.factory.access_control_factory import AccessControlFactory
from persisty.access_control.factory.permission_access_control_factory import (
    PermissionAccessControlFactory,
)
from persisty.context import PersistyContext
from persisty.impl.mem.mem_meta_storage import MemMetaStorage

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

    # TODO: Delete me! This is so wrong - it will probably fail on non mem databases anyway
    from tests.fixtures.number_name import NumberName
    from persisty.obj_storage.stored import get_storage_meta
    from marshy import dump
    from persisty.storage.batch_edit import Create
    from persisty.access_control.authorization import ROOT

    storage_meta = get_storage_meta(NumberName)
    persisty_context.get_meta_storage(ROOT).create(dump(storage_meta))

    storage = persisty_context.get_storage(storage_meta.name, ROOT)
    storage.edit_batch(
        [
            Create(dict(title=t, value=v))
            for v, t in enumerate(("Zero", "One", "Two", "Three", "Four", "Five"))
        ]
    )
