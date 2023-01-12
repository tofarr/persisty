import importlib
import logging
import os
import pkgutil
from dataclasses import dataclass, attr
from typing import Iterator

from servey.util import get_servey_main

from persisty.data_store.data_storage_factory_abc import DataStoreFactoryABC
from persisty.finder.store_factory_finder_abc import StoreFactoryFinderABC
from persisty.store.store_factory_abc import StoreFactoryABC

LOGGER = logging.getLogger(__name__)


@dataclass
class ModuleStorageFactoryFinder(StoreFactoryFinderABC):
    """
    Default implementation of action_ finder which searches for actions in a particular module
    """

    root_module_name: str = attr(
        default_factory=lambda: f"{os.environ.get('PERSISTY_MAIN') or get_servey_main()}.data_storage"
    )

    def find_storage_factories(self) -> Iterator[StoreFactoryABC]:
        try:
            module = importlib.import_module(self.root_module_name)
            # noinspection PyTypeChecker
            yield from _find_data_storage_factories_in_module(module)
        except ModuleNotFoundError:
            LOGGER.warning("error_finding_data_storage")


def _find_data_storage_factories_in_module(module) -> Iterator[StoreFactoryABC]:
    for name, value in module.__dict__.items():
        if isinstance(value, DataStoreFactoryABC):
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
        yield from _find_data_storage_factories_in_module(sub_module)
