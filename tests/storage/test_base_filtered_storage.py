from dataclasses import dataclass
from typing import Tuple, Optional
from unittest import TestCase

from marshy import dump
from marshy.types import ExternalItemType

from persisty.errors import PersistyError
from persisty.impl.mem.mem_storage import mem_storage
from persisty.obj_storage.stored import get_storage_meta
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_field import SearchOrderField
from persisty.storage.batch_edit import Create, Update, Delete
from persisty.storage.filtered_storage_abc import FilteredStorageABC
from persisty.storage.storage_abc import StorageABC
from tests.fixtures.number_name import NUMBER_NAMES, NumberName
from tests.fixtures.storage_tst_abc import StorageTstABC, ValueLessThanFilter
from tests.fixtures.super_bowl_results import SuperBowlResult, SUPER_BOWL_RESULTS


class TestBaseFilteredStorage(TestCase, StorageTstABC):
    def new_super_bowl_results_storage(self) -> StorageABC:
        storage = mem_storage(
            get_storage_meta(SuperBowlResult),
            {r.code: dump(r) for r in SUPER_BOWL_RESULTS},
        )
        return DummyFilteredStorage(storage)

    def new_number_name_storage(self) -> StorageABC:
        number_names = (dump(r) for r in NUMBER_NAMES)
        # noinspection PyTypeChecker
        storage = mem_storage(
            get_storage_meta(NumberName),
            {r["id"]: r for r in number_names},
        )
        return DummyFilteredStorage(storage)

    def test_search_custom_filter_invalid_key(self):
        # It would be nice to have this test in StorageTstABC, but it looks
        # like dynamodb does not support this behavior (Or at least moto doesn't
        # - I hope this is just a bug in moto.)
        # When an item from which an ExclusiveStartKey was generated is deleted,
        # you simply seem to be the first 3 results :(
        storage = self.new_number_name_storage()
        limit = 3
        kwargs = dict(
            search_filter=ValueLessThanFilter(10),
            search_order=SearchOrder((SearchOrderField("value"),)),
            limit=limit,
        )
        page_1 = storage.search(**kwargs)
        storage.delete(page_1.results[-1]["id"])
        kwargs["page_key"] = page_1.next_page_key
        with self.assertRaises(PersistyError):
            storage.search(**kwargs)

    def test_edit_batch_filtered(self):
        @dataclass
        class BatchRejectFilteredStorage(FilteredStorageABC):
            storage: StorageABC

            def get_storage(self) -> StorageABC:
                return self.storage

            def filter_create(
                self, item: ExternalItemType
            ) -> Optional[ExternalItemType]:
                return None

            # noinspection PyUnusedLocal
            # noinspection PyTypeChecker
            def filter_update(
                self, old_item: ExternalItemType, updates: ExternalItemType
            ) -> ExternalItemType:
                return None

            # noinspection PyUnusedLocal
            def allow_delete(self, item: ExternalItemType) -> bool:
                return False

        storage = BatchRejectFilteredStorage(self.new_number_name_storage())
        edits = [
            Create(
                dict(
                    id="00000000-0000-0000-0000-000000000150",
                    value=150,
                    title="One Hundred and Fifty",
                )
            ),
            Update(
                dict(
                    id="00000000-0000-0000-0000-000000000098",
                    value=98,
                    title="Updated Item",
                )
            ),
            Delete("00000000-0000-0000-0000-000000000098"),
        ]
        results = storage.edit_batch(edits)
        self.assertFalse(next((True for r in results if r.success), False))
        self.assertIsNone(
            storage.update(
                dict(
                    id="00000000-0000-0000-0000-000000000098",
                    value=101,
                    title="Updated Item",
                )
            )
        )
        self.assertEqual(99, storage.count())


@dataclass
class DummyFilteredStorage(FilteredStorageABC):
    storage: StorageABC

    def get_storage(self) -> StorageABC:
        return self.storage

    def filter_search_filter(
        self, search_filter: SearchFilterABC
    ) -> Tuple[SearchFilterABC, bool]:
        return search_filter, False
