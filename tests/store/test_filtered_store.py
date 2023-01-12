from datetime import datetime
from unittest import TestCase
from uuid import uuid4, UUID

from marshy import dump

from persisty.errors import PersistyError
from persisty.impl.mem.mem_store import mem_store
from persisty.obj_store.filter_factory import filter_factory
from persisty.obj_store.stored import get_meta
from persisty.store.batch_edit import Create, Update, Delete
from persisty.store.filtered_store import FilteredStore
from tests.fixtures.number_name import NUMBER_NAMES, NumberName
from tests.fixtures.store_tst_abc import StoreTstABC
from tests.fixtures.super_bowl_results import SuperBowlResult, SUPER_BOWL_RESULTS


class TestFilteredStore(TestCase, StoreTstABC):
    def new_super_bowl_results_store(self) -> FilteredStore:
        store = mem_store(
            get_meta(SuperBowlResult),
            {r.code: dump(r) for r in SUPER_BOWL_RESULTS},
        )
        filters = filter_factory(SuperBowlResult)
        return FilteredStore(store, filters.year.gte(1967))

    def new_number_name_store(self) -> FilteredStore:
        number_names = [dump(r) for r in NUMBER_NAMES]
        for i in range(100, 150):
            number_names.append(
                dump(
                    NumberName(
                        id=UUID(
                            "00000000-0000-0000-0000-000000000" + (str(1000 + i)[1:])
                        ),
                        title=str(i),
                        value=i,
                        created_at=datetime.fromtimestamp(0),
                        updated_at=datetime.fromtimestamp(0),
                    )
                )
            )
        # noinspection PyTypeChecker
        store = mem_store(
            get_meta(NumberName),
            {r["id"]: r for r in number_names},
        )
        filters = filter_factory(NumberName)
        return FilteredStore(store, filters.value.lt(100))

    def test_edit_batch_filtered(self):
        store = self.new_number_name_store()
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
                    id="00000000-0000-0000-0000-000000000101",
                    value=101,
                    title="Updated Item",
                )
            ),
            Update(
                dict(
                    id="00000000-0000-0000-0000-000000000098",
                    value=101,
                    title="Updated Item",
                )
            ),
            Delete("00000000-0000-0000-0000-000000000121"),
        ]
        results = store.edit_batch(edits)
        self.assertFalse(next((True for r in results if r.success), False))
        with self.assertRaises(PersistyError):
            store.update(
                dict(
                    id="00000000-0000-0000-0000-000000000098",
                    value=101,
                    title="Updated Item",
                )
            )
        self.assertIsNone(
            store.update(
                dict(
                    id="00000000-0000-0000-0000-000000000101",
                    value=98,
                    title="Updated Item",
                )
            )
        )
        self.assertEqual(99, store.count())
        self.assertEqual(149, store.store.count())
