from datetime import datetime

from persisty.finder.store_finder_abc import find_stores
from persisty.impl.default_store import DefaultStore
from persisty.impl.sqlalchemy.sqlalchemy_context_factory_abc import get_default_context
from persisty.io.seed import get_seed_data


def get_target_metadata():
    """
    Reference this in the alembic env.py
    """
    for store in find_stores():
        if isinstance(store, DefaultStore):
            store.get_store()  # make sure tables are registered
    target_metadata = get_default_context().meta_data
    return target_metadata


def add_seed_data(op):
    target_metadata = get_target_metadata()
    for store in find_stores():
        meta = store.get_meta()
        table = target_metadata.tables.get(meta.name)
        cols = list(table.columns)
        items = []
        for item in get_seed_data(meta.name):
            filtered_item = {**item}
            for col in cols:
                if col.type.python_type == datetime:
                    value = filtered_item.get(col.name)
                    if value:
                        filtered_item[col.name] = datetime.fromisoformat(value)
            items.append(filtered_item)
        op.bulk_insert(table, items)
