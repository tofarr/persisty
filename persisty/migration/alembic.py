from datetime import datetime

from persisty.factory.store_factory import StoreFactory
from persisty.finder.store_meta_finder_abc import find_store_meta
from persisty.impl.sqlalchemy.sqlalchemy_context_factory_abc import get_default_context
from persisty.io.seed import get_seed_data


def get_target_metadata():
    """
    Reference this in the alembic env.py
    """
    for store_meta in find_store_meta():
        if isinstance(store_meta.store_factory, StoreFactory):
            store_meta.store_factory.create(
                store_meta
            )  # make sure tables are registered
    target_metadata = get_default_context().meta_data
    return target_metadata


def add_seed_data(op):
    target_metadata = get_target_metadata()
    for store_meta in find_store_meta():
        seed_data = get_seed_data(store_meta.name)
        if not seed_data:
            continue
        table = target_metadata.tables.get(store_meta.name)
        cols = list(table.columns)
        items = []
        for item in seed_data:
            filtered_item = {**item}
            for col in cols:
                if col.type.python_type == datetime:
                    value = filtered_item.get(col.name)
                    if value:
                        # noinspection PyTypeChecker
                        filtered_item[col.name] = datetime.fromisoformat(value)
            items.append(filtered_item)
        op.bulk_insert(table, items)
