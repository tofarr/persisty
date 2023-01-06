from abc import ABC, abstractmethod
from copy import deepcopy
from datetime import datetime
from typing import Tuple

from dataclasses import dataclass
from uuid import uuid4

from marshy import ExternalType
from marshy.types import ExternalItemType

from persisty.errors import PersistyError
from persisty.obj_storage.filter_factory import filter_factory
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_field import SearchOrderField
from persisty.storage.batch_edit import BatchEdit
from persisty.field.field import Field
from persisty.field.field_filter import FieldFilter, FieldFilterOp
from persisty.storage.result_set import ResultSet
from persisty.storage.storage_abc import StorageABC
from tests.fixtures.super_bowl_results import SUPER_BOWL_RESULT_DICTS, SuperBowlResult
from tests.fixtures.number_name import NumberName, NUMBER_NAMES_DICTS


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
            "date": "1969-01-12T00:00:00+00:00",
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
                "date": "1973-01-14T00:00:00+00:00",
                "winner_code": "miami",
                "runner_up_code": "washington",
                "winner_score": 14,
                "runner_up_score": 7,
            },
            None,
            {
                "code": "ii",
                "year": 1968,
                "date": "1968-01-14T00:00:00+00:00",
                "winner_code": "green_bay",
                "runner_up_code": "oakland",
                "winner_score": 33,
                "runner_up_score": 14,
            },
            {
                "code": "xx",
                "year": 1986,
                "date": "1986-01-26T00:00:00+00:00",
                "winner_code": "chicago",
                "runner_up_code": "new_england",
                "winner_score": 46,
                "runner_up_score": 10,
            },
        ]
        self.assertEqual(expected, super_bowl_results)

    def test_create(self):
        storage = self.new_super_bowl_results_storage()
        self.spec_for_create(storage)

    def spec_for_create(self, storage):
        item = {
            "code": "c",
            "year": 2067,
            "date": "2067-01-15T00:00:00+00:00",
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
            "date": "2067-01-15T00:00:00+00:00",
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
            "date": "1967-01-15T00:00:00+00:00",
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
        self.spec_for_update(storage)

    def spec_for_update(self, storage):
        item = {
            "code": "li",
            "winner_code": "tom_brady_fan_club",
        }
        updated = storage.update(item)
        expected = {
            "code": "li",
            "year": 2017,
            "date": "2017-02-05T00:00:00+00:00",
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
        self.spec_for_update_missing_key(storage)

    def spec_for_update_missing_key(self, storage):
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
        with self.assertRaises(PersistyError):
            storage.update({"code": "i", "date": "not_a_date"})
        self.assertEqual(56, storage.count())
        read = storage.read("i")
        expected = {
            "code": "i",
            "year": 1967,
            "date": "1967-01-15T00:00:00+00:00",
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
            "date": "2017-02-05T00:00:00+00:00",
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
            "date": "2017-02-05T00:00:00+00:00",
            "winner_code": "new_england",
            "runner_up_code": "atlanta",
            "winner_score": 34,
            "runner_up_score": 28,
        }
        self.assertEqual(expected, item)

    def test_delete(self):
        storage = self.new_super_bowl_results_storage()
        self.assertTrue(storage.delete("lvi"))
        self.assertIsNone(storage.read("lvi"))

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

    def test_count_exclude_all(self):
        storage = self.new_super_bowl_results_storage()
        self.assertEqual(0, storage.count(EXCLUDE_ALL))

    def test_count_filter_id(self):
        storage = self.new_number_name_storage()
        from persisty.obj_storage.filter_factory import filter_factory

        filters = filter_factory(NumberName)
        num = storage.count(
            filters.id.eq("00000000-0000-0000-0000-000000000010")
            & filters.title.eq("Ten")
        )
        self.assertEqual(1, num)

    def test_count_custom_filter(self):
        @dataclass
        class StrLenFilter(SearchFilterABC):
            field_name: str
            required_length: int

            def validate_for_fields(self, fields: Tuple[Field, ...]) -> bool:
                return next((True for f in fields if f.name == self.field_name), False)

            def match(self, item: ExternalType, fields: Tuple[Field, ...]) -> bool:
                # noinspection PyTypeChecker
                return len(item[self.field_name]) == self.required_length

        storage = self.new_number_name_storage()
        from persisty.obj_storage.filter_factory import filter_factory

        num = storage.count(StrLenFilter("title", 4))
        self.assertEqual(3, num)  # Four, Five, Nine

    def test_count_non_indexed_filter(self):
        tag_storage = self.new_number_name_storage()
        filters = filter_factory(NumberName)
        count = tag_storage.count(filters.title.eq("Five") & filters.value.eq(5))
        self.assertEqual(1, count)

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

    def test_search(self):
        storage = self.new_number_name_storage()
        filters = filter_factory(NumberName)
        search_filter = filters.value.lt(50)
        search_order = filters.value.asc()
        page1 = storage.search(search_filter, search_order, None, 10)
        self.assertEqual(NUMBER_NAMES_DICTS[:10], page1.results)
        page2 = storage.search(search_filter, search_order, page1.next_page_key, 10)
        self.assertEqual(NUMBER_NAMES_DICTS[10:20], page2.results)

    def test_search_all_id(self):
        storage = self.new_number_name_storage()
        filters = filter_factory(NumberName)
        id_ = "00000000-0000-0000-0000-000000000001"
        loaded = list(storage.search_all(filters.id.eq(id_)))
        expected = [storage.read(id_)]
        self.assertEqual(expected, loaded)

    def test_search_all_id_title(self):
        storage = self.new_number_name_storage()
        filters = filter_factory(NumberName)
        id_ = "00000000-0000-0000-0000-000000000001"
        loaded = list(
            storage.search_all(filters.id.eq(id_) & filters.title.contains("One"))
        )
        expected = [storage.read(id_)]
        self.assertEqual(expected, loaded)

    def test_edit_all(self):
        storage = self.new_number_name_storage()
        edits = storage.search_all(FieldFilter("value", FieldFilterOp.gt, 3))
        edits = (BatchEdit(delete_key=n["id"]) for n in edits)
        list(storage.edit_all(edits))
        self.assertEqual(3, storage.count())
        edits = [
            BatchEdit(
                create_item=dict(
                    id="00000000-0000-0000-0001-000000000000",
                    title="Minus One",
                    value=-1,
                )
            ),
            BatchEdit(
                update_item=dict(
                    dict(
                        id="00000000-0000-0000-0002-000000000001", title="Not existing"
                    )
                )
            ),
            BatchEdit(
                update_item=dict(
                    id="00000000-0000-0000-0000-000000000001", title="First"
                )
            ),
            BatchEdit(delete_key="00000000-0000-0000-0002-000000000001"),
        ]
        now = datetime.now().isoformat()
        results = [r.success for r in storage.edit_all(edits)]
        self.assertEqual(results, [True, False, True, False])
        results = list(
            storage.search_all(search_order=SearchOrder((SearchOrderField("value"),)))
        )
        results = sorted(results, key=lambda r: r["value"])
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
                "created_at": "1970-01-01T00:00:00+00:00",
                "id": "00000000-0000-0000-0000-000000000001",
                "title": "First",
                "updated_at": results[1]["updated_at"],
                "value": 1,
            },
            {
                "created_at": "1970-01-01T00:00:00+00:00",
                "id": "00000000-0000-0000-0000-000000000002",
                "title": "Two",
                "updated_at": "1970-01-01T00:00:00+00:00",
                "value": 2,
            },
            {
                "created_at": "1970-01-01T00:00:00+00:00",
                "id": "00000000-0000-0000-0000-000000000003",
                "title": "Three",
                "updated_at": "1970-01-01T00:00:00+00:00",
                "value": 3,
            },
        ]
        self.assertEqual(expected_results, results)

    def test_edit_batch(self):
        storage = self.new_number_name_storage()
        edits = [BatchEdit(delete_key=n["id"]) for n in storage.search().results]
        results = list(storage.edit_batch(edits))
        for result in results:
            self.assertTrue(result.success)
        self.assertEqual(89, storage.count())

    def test_update_no_key(self):
        storage = self.new_number_name_storage()
        try:
            storage.update({}) and self.assertIsTrue(False)
        except PersistyError:
            pass

    def test_update_fail_filter(self):
        storage = self.new_number_name_storage()
        self.spec_for_update_fail_filter(storage)

    def spec_for_update_fail_filter(self, storage):
        item = storage.update(
            dict(id="00000000-0000-0000-0000-000000000001", name="Not One"),
            filter_factory(NumberName).title.ne("One"),
        )
        self.assertIsNone(item)

    def test_search_custom_filter_full_result_set(self):
        storage = self.new_number_name_storage()
        search_filter = ValueLessThanFilter(21)
        page_1 = storage.search(search_filter)
        self.assertEqual(list(range(1, 11)), list(i["value"] for i in page_1.results))
        page_2 = storage.search(
            search_filter=search_filter, page_key=page_1.next_page_key
        )
        self.assertEqual(list(range(11, 21)), list(i["value"] for i in page_2.results))
        page_3 = storage.search(
            search_filter=search_filter, page_key=page_2.next_page_key
        )
        self.assertEqual(ResultSet([]), page_3)

    def test_search_custom_filter_unfilled_result_set(self):
        storage = self.new_number_name_storage()
        limit = 3
        for less_than in range(1, 31):
            kwargs = dict(
                search_filter=ValueLessThanFilter(less_than),
                search_order=SearchOrder((SearchOrderField("value"),)),
                limit=limit,
            )
            index = 1
            while True:
                page = storage.search(**kwargs)
                expected_values = [
                    v for v in range(index, min(less_than, index + limit))
                ]
                values = [r["value"] for r in page.results]
                self.assertEqual(expected_values, values)
                if page.next_page_key:
                    kwargs["page_key"] = page.next_page_key
                    index += limit
                else:
                    break

    def test_edit_batch_errors(self):
        storage = self.new_number_name_storage()
        edits = [
            BatchEdit(
                create_item=dict(
                    id=NUMBER_NAMES_DICTS[1]["id"], value=-1, title="New Item"
                )
            ),
            BatchEdit(
                update_item=dict(id=str(uuid4()), value=-2, title="Updated Item")
            ),
            BatchEdit(delete_key=str(uuid4())),
            BatchEdit(),
        ]
        results = storage.edit_batch(edits)
        self.assertFalse(next((True for r in results if r.success), False))


@dataclass
class ValueLessThanFilter(SearchFilterABC):
    """
    Custom filter for testing - in reality you would use a FieldFilter
    for this as storage implementations would more easily be able to turn
    it into a native condition
    """

    value: int

    def validate_for_fields(self, fields: Tuple[Field, ...]) -> bool:
        return next((True for f in fields if f.name == "value"), False)

    def match(self, item: ExternalItemType, fields: Tuple[Field, ...]) -> bool:
        return item["value"] < self.value
