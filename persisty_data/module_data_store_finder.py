import importlib
import logging
import os
from dataclasses import dataclass, field
from typing import Iterator

from servey.util import get_servey_main

from persisty.finder.module_store_finder import find_instances_in_module
from persisty_data.data_store_abc import DataStoreABC
from persisty_data.data_store_finder_abc import DataStoreFinderABC

LOGGER = logging.getLogger(__name__)


@dataclass
class ModuleDataStoreFinder(DataStoreFinderABC):
    """
    Default implementation of data store factory finder which searches for actions in a particular module
    """

    root_module_name: str = field(
        default_factory=lambda: f"{os.environ.get('PERSISTY_MAIN') or get_servey_main()}.data_store"
    )

    def find_data_stores(self) -> Iterator[DataStoreABC]:
        try:
            module = importlib.import_module(self.root_module_name)
            # noinspection PyTypeChecker
            yield from find_instances_in_module(module, DataStoreABC)
        except ModuleNotFoundError:
            LOGGER.exception("error_finding_store")
