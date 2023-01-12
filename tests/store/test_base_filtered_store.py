from dataclasses import dataclass
from typing import Tuple, Optional
from unittest import TestCase

from marshy import dump
from marshy.types import ExternalItemType

from persisty.errors import PersistyError
from persisty.impl.mem.mem_store import mem_store
from persisty.obj_store.stored import get_meta
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderField
from persisty.store.batch_edit import Create, Update, Delete
from persisty.store.filtered_store_abc import FilteredStoreABC
from persisty.store.store_abc import StoreABC
from tests.fixtures.number_name import NUMBER_NAMES, NumberName
from tests.fixtures.store_tst_abc import StoreTstABC, ValueLessThanFilter
from tests.fixtures.super_bowl_results import SuperBowlResult, SUPER_BOWL_RESULTS


class TestBaseFilteredStore(TestCase, StoreTstABC):
    def new_super_bowl_results_store(self) -> StoreABC:
        store = mem_store(
            get_meta(SuperBowlResult),
            {r.code: dump(r) for r in SUPER_BOWL_RESULTS},
        )
        return DummyFilteredStore(store)

    def new_number_name_store(self) -> StoreABC:
        number_names = (dump(r) for r in NUMBER_NAMES)
        # noinspection PyTypeChecker
        store = mem_store(
            get_meta(NumberName),
            {r["id"]: r for r in number_names},
        )
        return DummyFilteredStore(store)

    def test_search_custom_filter_invalid_key(self):
        # It would be nice to have this test in StoreTstABC, but it looks
        # like dynamodb does not support this behavior (Or at least moto doesn't
        # - I hope this is just a bug in moto.)
        # When an item from which an ExclusiveStartKey was generated is deleted,
        # you simply seem to be the first 3 results :(
        store = self.new_number_name_store()
        limit = 3
        kwargs = dict(
            search_filter=ValueLessThanFilter(10),
            search_order=SearchOrder((SearchOrderField("value"),)),
            limit=limit,
        )
        page_1 = store.search(**kwargs)
        store.delete(page_1.results[-1]["id"])
        kwargs["page_key"] = page_1.next_page_key
        with self.assertRaises(PersistyError):
            store.search(**kwargs)

    def test_edit_batch_filtered(self):
        @dataclass
        class BatchRejectFilteredStore(FilteredStoreABC):
            store: StoreABC

            def get_store(self) -> StoreABC:
                return self.store

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

        store = BatchRejectFilteredStore(self.new_number_name_store())
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
        results = store.edit_batch(edits)
        self.assertFalse(next((True for r in results if r.success), False))
        self.assertIsNone(
            store.update(
                dict(
                    id="00000000-0000-0000-0000-000000000098",
                    value=101,
                    title="Updated Item",
                )
            )
        )
        self.assertEqual(99, store.count())


@dataclass
class DummyFilteredStore(FilteredStoreABC):
    store: StoreABC

    def get_store(self) -> StoreABC:
        return self.store

    def filter_search_filter(
        self, search_filter: SearchFilterABC
    ) -> Tuple[SearchFilterABC, bool]:
        return search_filter, False
