from dataclasses import dataclass
from unittest import TestCase

from marshy import dump

from persisty.impl.mem.mem_storage import mem_storage
from persisty.obj_storage.stored import get_storage_meta
from persisty.storage.storage_abc import StorageABC
from persisty.storage.wrapper_storage_abc import WrapperStorageABC
from tests.fixtures.number_name import NUMBER_NAMES, NumberName
from tests.fixtures.storage_tst_abc import StorageTstABC
from tests.fixtures.super_bowl_results import SuperBowlResult, SUPER_BOWL_RESULTS


class TestWrapperStorage(TestCase, StorageTstABC):

    def new_super_bowl_results_storage(self) -> StorageABC:
        storage = mem_storage(
            get_storage_meta(SuperBowlResult),
            {r.code: dump(r) for r in SUPER_BOWL_RESULTS},
        )
        return WrapperStorage(storage)

    def new_number_name_storage(self) -> StorageABC:
        number_names = (dump(r) for r in NUMBER_NAMES)
        # noinspection PyTypeChecker
        storage = mem_storage(
            get_storage_meta(NumberName),
            {r["id"]: r for r in number_names},
        )
        return WrapperStorage(storage)

@dataclass
class WrapperStorage(WrapperStorageABC):
    storage: StorageABC

    def get_storage(self) -> StorageABC:
        return self.storage
