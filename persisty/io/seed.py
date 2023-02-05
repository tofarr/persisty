import importlib
import json
import os
from typing import Iterable
from importlib import resources

import marshy

from persisty.store_meta import StoreMeta, T
from persisty.util import UNDEFINED


def get_seed_data(store_meta: StoreMeta) -> Iterable[T]:
    # noinspection PyBroadException
    try:
        seeds_package = os.environ.get("SEEDS_PACKAGE") or "seeds"
        seeds = importlib.import_module(seeds_package)
        items = getattr(seeds, store_meta.name, None)
        if items is not None:
            return items
        seed_data = resources.read_text(seeds_package, store_meta.name + ".json")
        seed_data = json.loads(seed_data)
        item_dataclass = store_meta.get_stored_dataclass()
        seed_items = []
        for data in seed_data:
            item = marshy.load(item_dataclass, data)
            for attr in store_meta.attrs:
                if attr.create_generator and getattr(item, attr.name) is UNDEFINED:
                    setattr(item, attr.name, attr.create_generator.transform(UNDEFINED))
            seed_items.append(item)
        return seed_items
    except Exception as e:
        print(e)
        return {}
