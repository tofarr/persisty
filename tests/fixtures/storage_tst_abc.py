from abc import ABC, abstractmethod
from copy import deepcopy
from datetime import datetime

from persisty.errors import PersistyError
from persisty.obj_storage.filter_factory import filter_factory
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_field import SearchOrderField
from persisty.storage.batch_edit import Delete, Update, Create
from persisty.storage.field.field_filter import FieldFilter, FieldFilterOp
from persisty.storage.storage_abc import StorageABC
from tests.fixtures.super_bowl_results import SUPER_BOWL_RESULT_DICTS, SuperBowlResult


# noinspection PyUnresolvedReferences
class StorageTstABC(ABC):
    """Tests which expect storage to have the bands data loaded"""

    @abstractmethod
    def new_super_bowl_results_storage(self) -> StorageABC:
        """Create a new storage object containing only Superbowl results"""

    @abstractmethod
    def new_number_name_storage(self) -> StorageABC:
        """Create a new storage object containing only Number Names"""

    def test_read(self):
        storage = self.new_super_bowl_results_storage()
        found = storage.read("iii")
        expected = {
            "code": "iii",
            "year": 1969,
            "date": "1969-01-12T00:00:00",
            "winner_code": "new_york_jets",
            "runner_up_code": "baltimore",
            "winner_score": 16,
            "runner_up_score": 7,
        }
        self.assertEqual(expected, found)

    def test_read_not_existing(self):
        storage = self.new_super_bowl_results_storage()
        found = storage.read("not_a_code")
        self.assertIsNone(found)

    def test_read_all(self):
        storage = self.new_super_bowl_results_storage()
        super_bowl_results = list(storage.read_all(("vii", "no-code-i", "ii", "xx")))
        expected = [
            {
                "code": "vii",
                "year": 1973,
                "date": "1973-01-14T00:00:00",
                "winner_code": "miami",
                "runner_up_code": "washington",
                "winner_score": 14,
                "runner_up_score": 7,
            },
            None,
            {
                "code": "ii",
                "year": 1968,
                "date": "1968-01-14T00:00:00",
                "winner_code": "green_bay",
                "runner_up_code": "oakland",
                "winner_score": 33,
                "runner_up_score": 14,
            },
            {
                "code": "xx",
                "year": 1986,
                "date": "1986-01-26T00:00:00",
                "winner_code": "chicago",
                "runner_up_code": "new_england",
                "winner_score": 46,
                "runner_up_score": 10,
            },
        ]
        self.assertEqual(expected, super_bowl_results)

    def test_create(self):
        storage = self.new_super_bowl_results_storage()
        item = {
            "code": "c",
            "year": 2067,
            "date": "2067-01-15T00:00:00",
            "winner_code": "robots",
            "runner_up_code": "humans",
            "winner_score": 1234,
            "runner_up_score": 0,
        }
        self.assertEqual(56, storage.count())
        created = storage.create(deepcopy(item))
        self.assertEqual(item, created)
        read = storage.read("c")
        self.assertEqual(item, read)
        self.assertEqual(57, storage.count())

    def test_create_invalid_schema(self):
        storage = self.new_super_bowl_results_storage()
        item = {
            "code": "c",
            "year": "not_a_year",
            "date": "2067-01-15T00:00:00",
            "winner_code": "robots",
            "runner_up_code": "humans",
            "winner_score": 1234,
            "runner_up_score": 0,
        }
        try:
            storage.create(deepcopy(item)) and self.assertTrue(False)
        except PersistyError:
            self.assertEqual(56, storage.count())

    def test_create_existing_key(self):
        storage = self.new_super_bowl_results_storage()
        item = {
            "code": "i",
            "year": 1971,
            "date": "1967-01-15T00:00:00",
            "winner_code": "green_bay",
            "runner_up_code": "kansas_city",
            "winner_score": 35,
            "runner_up_score": 10,
        }
        try:
            storage.create(deepcopy(item)) and self.assertTrue(False)
        except PersistyError:
            self.assertEqual(56, storage.count())

    def test_update(self):
        storage = self.new_super_bowl_results_storage()
        item = {
            "code": "li",
            "winner_code": "tom_brady_fan_club",
        }
        updated = storage.update(item)
        expected = {
            "code": "li",
            "year": 2017,
            "date": "2017-02-05T00:00:00",
            "winner_code": "tom_brady_fan_club",
            "runner_up_code": "atlanta",
            "winner_score": 34,
            "runner_up_score": 28,
        }
        self.assertEqual(expected, updated)
        self.assertEqual(56, storage.count())
        item = storage.read("li")
        self.assertEqual(expected, item)

    def test_update_missing_key(self):
        storage = self.new_super_bowl_results_storage()
        item = {
            "code": "not_a_key",
            "year": 1971,
        }
        updates = {**item}
        self.assertIsNone(storage.update(updates))
        self.assertEqual(56, storage.count())
        self.assertEqual(item, updates)

    def test_update_invalid_schema(self):
        storage = self.new_super_bowl_results_storage()
        try:
            storage.update({"code": "i", "date": "not_a_date"}) and self.assertTrue(
                False
            )
        except PersistyError:
            self.assertEqual(56, storage.count())
        read = storage.read("i")
        expected = {
            "code": "i",
            "year": 1967,
            "date": "1967-01-15T00:00:00",
            "winner_code": "green_bay",
            "runner_up_code": "kansas_city",
            "winner_score": 35,
            "runner_up_score": 10,
        }
        self.assertEqual(56, storage.count())
        self.assertEqual(expected, read)

    def test_update_valid_filter(self):
        storage = self.new_super_bowl_results_storage()
        item = {
            "code": "li",
            "winner_code": "tom_brady_fan_club",
        }
        self.assertTrue(
            storage.update(item, FieldFilter("year", FieldFilterOp.eq, 2017))
        )
        item = storage.read("li")
        expected = {
            "code": "li",
            "year": 2017,
            "date": "2017-02-05T00:00:00",
            "winner_code": "tom_brady_fan_club",
            "runner_up_code": "atlanta",
            "winner_score": 34,
            "runner_up_score": 28,
        }
        self.assertEqual(expected, item)

    def test_update_invalid_filter(self):
        storage = self.new_super_bowl_results_storage()
        item = {
            "code": "li",
            "winner_code": "tom_brady_fan_club",
        }
        self.assertIsNone(
            storage.update(item, FieldFilter("year", FieldFilterOp.eq, 2018))
        )
        item = storage.read("li")
        expected = {
            "code": "li",
            "year": 2017,
            "date": "2017-02-05T00:00:00",
            "winner_code": "new_england",
            "runner_up_code": "atlanta",
            "winner_score": 34,
            "runner_up_score": 28,
        }
        self.assertEqual(expected, item)

    def test_delete(self):
        storage = self.new_super_bowl_results_storage()
        self.assertTrue(storage.read("lvi"))

    def test_delete_missing_key(self):
        storage = self.new_super_bowl_results_storage()
        self.assertFalse(storage.delete("missing_key"))

    def test_count(self):
        storage = self.new_super_bowl_results_storage()
        self.assertEqual(56, storage.count())
        filters = filter_factory(SuperBowlResult)
        self.assertEqual(
            20, storage.count(filters.year.gte(1984) & filters.year.lt(2004))
        )
        self.assertEqual(6, storage.count(filters.winner_code.contains("new_england")))
        self.assertEqual(0, storage.count(FieldFilter("year", FieldFilterOp.lt, 1967)))

    def test_count_invalid_field_filter(self):
        storage = self.new_super_bowl_results_storage()
        try:
            storage.count(
                FieldFilter("non_field", FieldFilterOp.gte, 1984)
            ) and self.assertTrue(False)
        except PersistyError:
            pass

    def test_search_all(self):
        storage = self.new_super_bowl_results_storage()
        self.assertEqual(SUPER_BOWL_RESULT_DICTS, list(storage.search_all()))
        filters = filter_factory(SuperBowlResult)
        self.assertEqual(
            SUPER_BOWL_RESULT_DICTS[17:37],
            list(storage.search_all(filters.year.gte(1984) & filters.year.lt(2004))),
        )
        self.assertEqual(
            [r for r in SUPER_BOWL_RESULT_DICTS if r["winner_code"] == "new_england"],
            list(
                storage.search_all(
                    FieldFilter("winner_code", FieldFilterOp.contains, "new_england")
                )
            ),
        )
        self.assertEqual(
            [], list(storage.search_all(FieldFilter("year", FieldFilterOp.lt, 1967)))
        )

    def test_edit_all(self):
        storage = self.new_number_name_storage()
        edits = storage.search_all(FieldFilter("value", FieldFilterOp.gt, 3))
        edits = (Delete(n["id"]) for n in edits)
        list(storage.edit_all(edits))
        self.assertEqual(3, storage.count())
        edits = [
            Create(
                dict(
                    id="00000000-0000-0000-0001-000000000000",
                    title="Minus One",
                    value=-1,
                )
            ),
            Update(
                dict(id="00000000-0000-0000-0002-000000000001", title="Not existing")
            ),
            Update(dict(id="00000000-0000-0000-0000-000000000001", title="First")),
            Delete("00000000-0000-0000-0002-000000000001"),
        ]
        now = str(datetime.now())
        results = [r.success for r in storage.edit_all(edits)]
        self.assertEqual(results, [True, False, True, False])
        results = list(
            storage.search_all(search_order=SearchOrder((SearchOrderField("value"),)))
        )
        results = sorted(results, key=lambda r: r['value'])
        self.assertTrue(results[0]["created_at"] >= now)
        self.assertTrue(results[0]["updated_at"] >= now)
        self.assertTrue(results[1]["updated_at"] >= now)
        expected_results = [
            {
                "created_at": results[0]["created_at"],
                "id": "00000000-0000-0000-0001-000000000000",
                "title": "Minus One",
                "updated_at": results[0]["updated_at"],
                "value": -1,
            },
            {
                "created_at": "1969-12-31T17:00:00",
                "id": "00000000-0000-0000-0000-000000000001",
                "title": "First",
                "updated_at": results[1]["updated_at"],
                "value": 1,
            },
            {
                "created_at": "1969-12-31T17:00:00",
                "id": "00000000-0000-0000-0000-000000000002",
                "title": "Two",
                "updated_at": "1969-12-31T17:00:00",
                "value": 2,
            },
            {
                "created_at": "1969-12-31T17:00:00",
                "id": "00000000-0000-0000-0000-000000000003",
                "title": "Three",
                "updated_at": "1969-12-31T17:00:00",
                "value": 3,
            },
        ]
        self.assertEqual(expected_results, results)
