import json
import os
from itertools import islice
from os.path import exists
from pathlib import Path
from typing import Type, List, Iterator

import marshy
from marshy.types import ExternalItemType

from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.finder.store_finder_abc import find_stores
from persisty.batch_edit import BatchEdit
from persisty.impl.default_store import DefaultStore
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta


def export_all(directory: str):
    """
    Export all store to yml files
    """
    for store in find_stores():
        export_meta(directory, store)
        export_content(directory, store)


def export_meta(directory: str, store: StoreABC):
    """
    Export store to json files
    """
    store_meta = store.get_meta()
    path = Path(directory, store_meta.name, "meta.json")
    path.parent.mkdir(exist_ok=True, parents=True)
    meta = marshy.dump(store_meta)
    with open(path, "w") as f:
        json.dump(meta, f)


def export_content(directory: str, store: StoreABC, page_size: int = 500):
    """
    Export store content to json files
    """
    store_meta = store.get_meta()
    path = Path(directory, store_meta.name)
    path.mkdir(exist_ok=True, parents=True)
    results = store.search_all()
    index = 1
    while True:
        batch = list(islice(results, page_size))
        if not batch:
            return
        path = Path(directory, store_meta.name, str(index) + ".json")
        with open(path, "w") as f:
            json.dump(batch, f)
        index += 1


def import_all(
    directory: str, store_type: Type = DefaultStore
) -> List[StoreFactoryABC]:
    results = []
    for store_name in os.listdir(directory):
        store_factory = import_store(directory, store_name, store_type)
        results.append(store_factory)
        import_content(directory, store_factory)
    return results


def import_store(
    directory: str,
    store_name: str,
    store_type: Type = DefaultStore,
) -> StoreFactoryABC:
    store_meta = import_meta(directory, store_name)
    factory = store_type(store_meta)
    return factory


def import_meta(directory: str, store_name: str) -> StoreMeta:
    path = Path(directory, store_name, "meta.yml")
    with open(path, "r") as f:
        meta = json.load(f)
    store_meta = marshy.load(StoreMeta, meta)
    return store_meta


def import_content(directory: str, store: StoreABC):
    store_meta = store.get_meta()
    directory = Path(directory, store_meta.name)
    to_key_str = store_meta.key_config.to_key_str
    existing_item_edits = (
        BatchEdit(delete_key=to_key_str(i)) for i in store.search_all()
    )
    store.edit_all(existing_item_edits)
    items = read_all_items(directory)
    store.edit_all((BatchEdit(create_item=i) for i in items))


def read_all_items(directory: Path) -> Iterator[ExternalItemType]:
    index = 1
    while True:
        file = Path(directory, str(index) + ".yml")
        if not exists(file):
            return
        with open(file, "r") as f:
            # noinspection PyTypeChecker
            items = json.loads(f)
            yield from items
            index += 1
