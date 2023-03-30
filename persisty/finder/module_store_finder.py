import importlib
import logging
import os
import pkgutil
from dataclasses import dataclass, field
from typing import Iterator

from servey.util import get_servey_main

from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.finder.store_finder_abc import StoreFactoryFinderABC
from persisty.store.store_abc import StoreABC

LOGGER = logging.getLogger(__name__)


@dataclass
class ModuleStoreFinder(StoreFactoryFinderABC):
    """
    Default implementation of store factory finder which searches for actions in a particular module
    """

    root_module_name: str = field(
        default_factory=lambda: f"{os.environ.get('PERSISTY_MAIN') or get_servey_main()}.store"
    )

    def find_stores(self) -> Iterator[StoreABC]:
        module = importlib.import_module(self.root_module_name)
        # noinspection PyTypeChecker
        yield from find_instances_in_module(module, StoreABC)

    def find_store_factories(self) -> Iterator[StoreFactoryABC]:
        module = importlib.import_module(self.root_module_name)
        # noinspection PyTypeChecker
        yield from find_instances_in_module(module, StoreFactoryABC)


def find_instances_in_module(module, type_) -> Iterator:
    for name, value in list(module.__dict__.items()):
        if isinstance(value, type_):
            yield value
    if not hasattr(module, "__path__"):
        return  # Module was not a package...
    paths = []
    paths.extend(module.__path__)
    module_infos = list(pkgutil.walk_packages(paths))
    for module_info in module_infos:
        sub_module_name = module.__name__ + "." + module_info.name
        sub_module = importlib.import_module(sub_module_name)
        # noinspection PyTypeChecker
        yield from find_instances_in_module(sub_module, type_)
