import json
import os
from itertools import islice
from os.path import exists
from pathlib import Path
from typing import Type, List, Iterator

import marshy
from marshy.types import ExternalItemType

from build.lib.persisty.impl.default_store import DefaultStore
from persisty.finder.stored_finder_abc import find_stored
from persisty.batch_edit import BatchEdit
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta


def export_all(directory: str):
    """
    Export all store to yml files
    """
    for store_meta in find_stored():
        export_meta(directory, store_meta)
        export_content(directory, store_meta)


def export_meta(directory: str, store_meta: StoreMeta):
    """
    Export store to json files
    """
    path = Path(directory, store_meta.name, "meta.json")
    path.parent.mkdir(exist_ok=True, parents=True)
    meta = marshy.dump(store_meta)
    # pylint: disable=W1514
    with open(path, "w") as f:
        json.dump(meta, f)


def export_content(directory: str, store_meta: StoreMeta, page_size: int = 500):
    """
    Export store content to json files
    """
    store = store_meta.store_factory.create()
    path = Path(directory, store_meta.name)
    path.mkdir(exist_ok=True, parents=True)
    results = store.search_all()
    index = 1
    while True:
        batch = list(islice(results, page_size))
        if not batch:
            return
        path = Path(directory, store_meta.name, str(index) + ".json")
        # pylint: disable=W1514
        with open(path, "w") as f:
            json.dump(batch, f)
        index += 1


def import_all(directory: str, store_type: Type = DefaultStore) -> List[StoreABC]:
    results = []
    for store_name in os.listdir(directory):
        store = import_store(directory, store_name, store_type)
        results.append(store)
        import_content(directory, store)
    return results


def import_store(
    directory: str,
    store_name: str,
    store_type: Type = DefaultStore,
) -> StoreABC:
    store_meta = import_meta(directory, store_name)
    store = store_type(store_meta)
    return store


def import_meta(directory: str, store_name: str) -> StoreMeta:
    path = Path(directory, store_name, "meta.yml")
    # pylint: disable=W1514
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
        # pylint: disable=W1514
        with open(file, "r") as f:
            # noinspection PyTypeChecker
            items = json.loads(f)
            yield from items
            index += 1
