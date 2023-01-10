import importlib
import logging
import os
import pkgutil
from dataclasses import dataclass, field
from typing import Iterator

from servey.util import get_servey_main

from persisty.finder.storage_factory_finder_abc import StorageFactoryFinderABC
from persisty.secured.secured_storage_factory_abc import SecuredStorageFactoryABC
from persisty.storage.storage_factory_abc import StorageFactoryABC

LOGGER = logging.getLogger(__name__)


@dataclass
class ModuleStorageFactoryFinder(StorageFactoryFinderABC):
    """
    Default implementation of action_ finder which searches for actions in a particular module
    """

    root_module_name: str = field(
        default_factory=lambda: f"{os.environ.get('PERSISTY_MAIN') or get_servey_main()}.storage"
    )

    def find_storage_factories(self) -> Iterator[StorageFactoryABC]:
        try:
            module = importlib.import_module(self.root_module_name)
            # noinspection PyTypeChecker
            yield from find_instances_in_module(module, StorageFactoryABC)
        except ModuleNotFoundError:
            LOGGER.exception("error_finding_storage")

    def find_secured_storage_factories(self) -> Iterator[SecuredStorageFactoryABC]:
        try:
            module = importlib.import_module(self.root_module_name)
            # noinspection PyTypeChecker
            yield from find_instances_in_module(module, SecuredStorageFactoryABC)
        except ModuleNotFoundError:
            LOGGER.exception("error_finding_storage")


def find_instances_in_module(module, type_) -> Iterator[StorageFactoryABC]:
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
