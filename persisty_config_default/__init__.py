from persisty.context import PersistyContext
from persisty.impl.mem.mem_meta_storage import MemMetaStorage

priority = 100


def create_meta_storage():
    """The default is MemMetaStorage. This can be overridden to provide Dynamodb, SQL, or other composite options."""
    # TODO: A Sqlite / Sqlalchemy default would be interesting
    return MemMetaStorage()


def configure_context(persisty_context: PersistyContext):
    pass
