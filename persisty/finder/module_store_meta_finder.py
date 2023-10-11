import importlib
import os
import pkgutil
from dataclasses import field, dataclass
from typing import Iterator

from servey.util import get_servey_main

from persisty.finder.store_meta_finder_abc import StoreMetaFinderABC
from persisty.store_meta import StoreMeta, get_meta


@dataclass
class ModuleStoreMetaFinder(StoreMetaFinderABC):
    root_module_name: str = field(
        default_factory=lambda: f"{os.environ.get('PERSISTY_MAIN') or get_servey_main()}.store"
    )

    def find_store_meta(self) -> Iterator[StoreMeta]:
        module = importlib.import_module(self.root_module_name)
        # noinspection PyTypeChecker
        yield from find_in_module(module)


def find_in_module(module) -> Iterator[StoreMeta]:
    yield from get_from_module(module)
    if not hasattr(module, "__path__"):
        return  # Module was not a package...
    paths = []
    paths.extend(module.__path__)
    module_infos = list(pkgutil.walk_packages(paths))
    for module_info in module_infos:
        sub_module_name = module.__name__ + "." + module_info.name
        sub_module = importlib.import_module(sub_module_name)
        # noinspection PyTypeChecker
        yield from find_in_module(sub_module)


def get_from_module(module) -> Iterator[StoreMeta]:
    for value in list(module.__dict__.values()):
        store_meta = get_meta(value)
        if store_meta:
            yield store_meta
