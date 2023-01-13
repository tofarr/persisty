import importlib
import json
import os
from typing import Iterable
from importlib import resources

import marshy
from marshy.types import ExternalItemType

from persisty.store_meta import StoreMeta, T


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
        # noinspection PyTypeChecker
        seed_items = [marshy.load(item_dataclass, d) for d in seed_data]
        return seed_items
    except Exception as e:
        print(e)
        return {}
