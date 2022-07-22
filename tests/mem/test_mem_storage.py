from unittest import TestCase

from marshy import dump
from moto import mock_dynamodb

from persisty.impl.mem.mem_storage import mem_storage
from persisty.obj_storage.filter_factory import filter_factory
from persisty.obj_storage.stored import get_storage_meta
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.storage.storage_abc import StorageABC
from tests.fixtures.number_name import NumberName, NUMBER_NAMES
from tests.fixtures.super_bowl_results import SuperBowlResult, SUPER_BOWL_RESULTS, SUPER_BOWL_RESULT_DICTS
from tests.fixtures.storage_tst_abc import StorageTstABC


@mock_dynamodb
class TestMemStorage(TestCase, StorageTstABC):

    def new_super_bowl_results_storage(self) -> StorageABC:
        storage = mem_storage(
            get_storage_meta(SuperBowlResult),
            {r.code: dump(r) for r in SUPER_BOWL_RESULTS},
        )
        return storage

    def new_number_name_storage(self) -> StorageABC:
        number_names = (dump(r) for r in NUMBER_NAMES)
        # noinspection PyTypeChecker
        storage = mem_storage(
            get_storage_meta(NumberName),
            {r["id"]: r for r in number_names},
        )
        return storage

    def test_search_all_sorted(self):
        storage = self.new_super_bowl_results_storage()
        filters = filter_factory(SuperBowlResult)
        self.assertEqual(
            list(reversed(SUPER_BOWL_RESULT_DICTS)),
            list(storage.search_all(INCLUDE_ALL, filters.year.desc())),
        )
        self.assertEqual(
            list(reversed(SUPER_BOWL_RESULT_DICTS[17:37])),
            list(
                storage.search_all(
                    filters.year.gte(1984) & filters.year.lt(2004), filters.year.desc()
                )
            ),
        )
