from marshy import dump

from persisty.impl.mem.mem_storage import mem_storage
from persisty.obj_storage.stored import get_storage_meta
from persisty.storage.storage_abc import StorageABC
from tests.fixtures.super_bowl_results import SuperBowlResult, SUPER_BOWL_RESULTS
from tests.storage_test_abc import StorageTestABC


class TestMemBandStorage(StorageTestABC):

    def new_super_bowl_results_storage(self) -> StorageABC:
        storage = mem_storage(
            get_storage_meta(SuperBowlResult),
            {r.code: dump(r) for r in SUPER_BOWL_RESULTS},
        )
        return storage
