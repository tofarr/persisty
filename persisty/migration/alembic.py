from persisty.finder.store_finder_abc import find_stores
from persisty.impl.default_store import DefaultStore
from persisty.impl.sqlalchemy.sqlalchemy_context_factory_abc import get_default_context


def get_target_metadata():
    """
    Reference this in the alembic env.py
    """
    for store in find_stores():
        if isinstance(store, DefaultStore):
            store.get_store()  # make sure tables are registered
    result = get_default_context().meta_data
    return result
