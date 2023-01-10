import json
import os
from itertools import islice
from os.path import exists
from pathlib import Path
from typing import Type, List, Iterator

import marshy
from marshy.types import ExternalItemType

from persisty.finder.storage_factory_finder_abc import find_storage_factories
from persisty.impl.default_storage_factory import DefaultStorageFactory
from persisty.storage.batch_edit import BatchEdit
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_factory_abc import StorageFactoryABC
from persisty.storage.storage_meta import StorageMeta


def export_all(directory: str):
    """
    Export all storage to yml files
    """
    for factory in find_storage_factories():
        storage = factory.create()
        export_meta(directory, storage)
        export_content(directory, storage)


def export_meta(directory: str, storage: StorageABC):
    """
    Export storage to json files
    """
    storage_meta = storage.get_storage_meta()
    path = Path(directory, storage_meta.name, "meta.json")
    path.parent.mkdir(exist_ok=True, parents=True)
    meta = marshy.dump(storage_meta)
    with open(path, "w") as f:
        json.dump(meta, f)


def export_content(directory: str, storage: StorageABC, page_size: int = 500):
    """
    Export storage content to json files
    """
    storage_meta = storage.get_storage_meta()
    path = Path(directory, storage_meta.name)
    path.mkdir(exist_ok=True, parents=True)
    results = storage.search_all()
    index = 1
    while True:
        batch = list(islice(results, page_size))
        if not batch:
            return
        path = Path(directory, storage_meta.name, str(index) + ".json")
        with open(path, "w") as f:
            json.dump(batch, f)
        index += 1


def import_all(directory: str, storage_factory_type: Type = DefaultStorageFactory) -> List[StorageFactoryABC]:
    results = []
    for storage_name in os.listdir(directory):
        storage_factory = import_storage_factory(
            directory, storage_name, storage_factory_type
        )
        results.append(storage_factory)
        import_content(directory, storage_factory)
    return results


def import_storage_factory(
    directory: str,
    storage_name: str,
    storage_factory_type: Type = DefaultStorageFactory,
) -> StorageFactoryABC:
    storage_meta = import_meta(directory, storage_name)
    factory = storage_factory_type(storage_meta)
    return factory


def import_meta(directory: str, storage_name: str) -> StorageMeta:
    path = Path(directory, storage_name, "meta.yml")
    with open(path, "r") as f:
        meta = json.load(f)
    storage_meta = marshy.load(StorageMeta, meta)
    return storage_meta


def import_content(directory: str, storage_factory: StorageFactoryABC):
    storage_meta = storage_factory.get_storage_meta()
    directory = Path(directory, storage_meta.name)
    storage = storage_factory.create()
    to_key_str = storage_meta.key_config.to_key_str
    existing_item_edits = (BatchEdit(delete_key=to_key_str(i)) for i in storage.search_all())
    storage.edit_all(existing_item_edits)
    items = read_all_items(directory)
    storage.edit_all((BatchEdit(create_item=i) for i in items))


def read_all_items(directory: Path) -> Iterator[ExternalItemType]:
    index = 1
    while True:
        file = Path(directory, str(index) + '.yml')
        if not exists(file):
            return
        with open(file, "r") as f:
            items = json.loads(f)
            yield from items
            index += 1
