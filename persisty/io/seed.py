import importlib
import json
import os
from typing import Iterable
from importlib import resources

from marshy.types import ExternalItemType

from persisty.store_meta import StoreMeta


def get_seed_data(storage_meta: StoreMeta) -> Iterable[ExternalItemType]:
    # noinspection PyBroadException
    try:
        seeds_package = os.environ.get("SEEDS_PACKAGE") or "seeds"
        seeds = importlib.import_module(seeds_package)
        seed_data = getattr(seeds, storage_meta.name, None)
        if seed_data is not None:
            return seed_data
        seed_data = resources.read_text(seeds_package, storage_meta.name + ".json")
        seed_data = json.loads(seed_data)
        return seed_data
    except Exception as e:
        print(e)
        return {}
