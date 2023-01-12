import importlib
import logging
import os
import pkgutil
from dataclasses import dataclass, field
from typing import Iterator

from servey.util import get_servey_main

from persisty.finder.store_factory_finder_abc import StoreFactoryFinderABC
from persisty.secured.secured_store_factory_abc import SecuredStoreFactoryABC
from persisty.store.store_factory_abc import StoreFactoryABC

LOGGER = logging.getLogger(__name__)


@dataclass
class ModuleStoreFactoryFinder(StoreFactoryFinderABC):
    """
    Default implementation of action_ finder which searches for actions in a particular module
    """

    root_module_name: str = field(
        default_factory=lambda: f"{os.environ.get('PERSISTY_MAIN') or get_servey_main()}.store"
    )

    def find_store_factories(self) -> Iterator[StoreFactoryABC]:
        try:
            module = importlib.import_module(self.root_module_name)
            # noinspection PyTypeChecker
            yield from find_instances_in_module(module, StoreFactoryABC)
        except ModuleNotFoundError:
            LOGGER.exception("error_finding_store")

    def find_secured_store_factories(self) -> Iterator[SecuredStoreFactoryABC]:
        try:
            module = importlib.import_module(self.root_module_name)
            # noinspection PyTypeChecker
            yield from find_instances_in_module(module, SecuredStoreFactoryABC)
        except ModuleNotFoundError:
            LOGGER.exception("error_finding_store")


def find_instances_in_module(module, type_) -> Iterator[StoreFactoryABC]:
    for name, value in module.__dict__.items():
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
