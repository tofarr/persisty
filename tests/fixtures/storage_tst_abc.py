from abc import ABC, abstractmethod
from copy import deepcopy
from datetime import datetime, timezone
from typing import Tuple

from dataclasses import dataclass, replace
from uuid import uuid4, UUID

import marshy
from dateutil.relativedelta import relativedelta

from persisty.attr.attr import Attr
from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.batch_edit import BatchEdit
from persisty.errors import PersistyError
from persisty.result_set import ResultSet
from persisty.search_filter.filter_factory import filter_factory
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr
from persisty.store.store_abc import StoreABC, T
from tests.fixtures.super_bowl_results import SUPER_BOWL_RESULTS, SuperBowlResult
from tests.fixtures.number_name import NumberName, NUMBER_NAMES


# noinspection PyUnresolvedReferences
class StoreTstABC(ABC):
    """Tests which expect store to have the bands data loaded"""

    @abstractmethod
    def new_super_bowl_results_store(self) -> StoreABC:
        """Create a new store object containing only Superbowl results"""

    @abstractmethod
    def new_number_name_store(self) -> StoreABC:
        """Create a new store object containing only Number Names"""

    def test_read(self):
        store = self.new_super_bowl_results_store()
        found = store.read("iii")
        expected = marshy.load(
            SuperBowlResult,
            {
                "code": "iii",
                "year": 1969,
                "date": "1969-01-12T00:00:00+00:00",
                "winner_code": "new_york_jets",
                "runner_up_code": "baltimore",
                "winner_score": 16,
                "runner_up_score": 7,
            },
        )
        self.assertEqual(expected, found)

    def test_read_not_existing(self):
        store = self.new_super_bowl_results_store()
        found = store.read("not_a_code")
        self.assertIsNone(found)

    def test_read_all(self):
        store = self.new_super_bowl_results_store()
        super_bowl_results = list(store.read_all(("vii", "no-code-i", "ii", "xx")))
        expected = [
            marshy.load(
                SuperBowlResult,
                {
                    "code": "vii",
                    "year": 1973,
                    "date": "1973-01-14T00:00:00+00:00",
                    "winner_code": "miami",
                    "runner_up_code": "washington",
                    "winner_score": 14,
                    "runner_up_score": 7,
                },
            ),
            None,
            marshy.load(
                SuperBowlResult,
                {
                    "code": "ii",
                    "year": 1968,
                    "date": "1968-01-14T00:00:00+00:00",
                    "winner_code": "green_bay",
                    "runner_up_code": "oakland",
                    "winner_score": 33,
                    "runner_up_score": 14,
                },
            ),
            marshy.load(
                SuperBowlResult,
                {
                    "code": "xx",
                    "year": 1986,
                    "date": "1986-01-26T00:00:00+00:00",
                    "winner_code": "chicago",
                    "runner_up_code": "new_england",
                    "winner_score": 46,
                    "runner_up_score": 10,
                },
            ),
        ]
        self.assertEqual(expected, super_bowl_results)

    def test_create(self):
        store = self.new_super_bowl_results_store()
        self.spec_for_create(store)

    def spec_for_create(self, store):
        item = marshy.load(
            SuperBowlResult,
            {
                "code": "c",
                "year": 2067,
                "date": "2067-01-15T00:00:00+00:00",
                "winner_code": "robots",
                "runner_up_code": "humans",
                "winner_score": 1234,
                "runner_up_score": 0,
            },
        )
        self.assertEqual(56, store.count())
        created = store.create(item)
        self.assertEqual(item, created)
        read = store.read("c")
        self.assertEqual(item, read)
        self.assertEqual(57, store.count())

    def test_create_invalid_schema(self):
        store = self.new_super_bowl_results_store()
        item = SuperBowlResult(
            **{
                "code": "c",
                "year": "not_a_year",
                "date": datetime.fromisoformat("2067-01-15T00:00:00+00:00"),
                "winner_code": "robots",
                "runner_up_code": "humans",
                "winner_score": 1234,
                "runner_up_score": 0,
            }
        )
        try:
            store.create(deepcopy(item)) and self.assertTrue(False)
        except PersistyError:
            self.assertEqual(56, store.count())

    def test_create_existing_key(self):
        store = self.new_super_bowl_results_store()
        item = marshy.load(
            SuperBowlResult,
            {
                "code": "i",
                "year": 1971,
                "date": "1967-01-15T00:00:00+00:00",
                "winner_code": "green_bay",
                "runner_up_code": "kansas_city",
                "winner_score": 35,
                "runner_up_score": 10,
            },
        )
        try:
            store.create(deepcopy(item)) and self.assertTrue(False)
        except PersistyError:
            self.assertEqual(56, store.count())

    def test_update(self):
        store = self.new_super_bowl_results_store()
        self.spec_for_update(store)

    def spec_for_update(self, store):
        item = marshy.load(
            SuperBowlResult,
            {
                "code": "li",
                "winner_code": "tom_brady_fan_club",
            },
        )
        updated = store.update(item)
        expected = marshy.load(
            SuperBowlResult,
            {
                "code": "li",
                "year": 2017,
                "date": "2017-02-05T00:00:00+00:00",
                "winner_code": "tom_brady_fan_club",
                "runner_up_code": "atlanta",
                "winner_score": 34,
                "runner_up_score": 28,
            },
        )
        self.assertEqual(expected, updated)
        self.assertEqual(56, store.count())
        item = store.read("li")
        self.assertEqual(expected, item)

    def test_update_missing_key(self):
        store = self.new_super_bowl_results_store()
        self.spec_for_update_missing_key(store)

    def spec_for_update_missing_key(self, store):
        item = SuperBowlResult(
            code="not_a_key",
            year=1971,
        )
        # noinspection PyDataclass
        updates = replace(item)
        self.assertIsNone(store.update(updates))
        self.assertEqual(56, store.count())
        self.assertEqual(item, updates)

    def test_update_invalid_schema(self):
        store = self.new_super_bowl_results_store()
        with self.assertRaises(AttributeError):
            store.update(SuperBowlResult(code="i", date="not_a_date"))
        self.assertEqual(56, store.count())
        read = store.read("i")
        expected = marshy.load(
            SuperBowlResult,
            {
                "code": "i",
                "year": 1967,
                "date": "1967-01-15T00:00:00+00:00",
                "winner_code": "green_bay",
                "runner_up_code": "kansas_city",
                "winner_score": 35,
                "runner_up_score": 10,
            },
        )
        self.assertEqual(56, store.count())
        self.assertEqual(expected, read)

    def test_update_valid_filter(self):
        store = self.new_super_bowl_results_store()
        item = SuperBowlResult(
            code="li",
            winner_code="tom_brady_fan_club",
        )
        self.assertTrue(store.update(item, AttrFilter("year", AttrFilterOp.eq, 2017)))
        item = store.read("li")
        expected = marshy.load(
            SuperBowlResult,
            {
                "code": "li",
                "year": 2017,
                "date": "2017-02-05T00:00:00+00:00",
                "winner_code": "tom_brady_fan_club",
                "runner_up_code": "atlanta",
                "winner_score": 34,
                "runner_up_score": 28,
            },
        )
        self.assertEqual(expected, item)

    def test_update_invalid_filter(self):
        store = self.new_super_bowl_results_store()
        item = SuperBowlResult(code="li", winner_code="tom_brady_fan_club")
        self.assertIsNone(store.update(item, AttrFilter("year", AttrFilterOp.eq, 2018)))
        item = store.read("li")
        expected = marshy.load(
            SuperBowlResult,
            {
                "code": "li",
                "year": 2017,
                "date": "2017-02-05T00:00:00+00:00",
                "winner_code": "new_england",
                "runner_up_code": "atlanta",
                "winner_score": 34,
                "runner_up_score": 28,
            },
        )
        self.assertEqual(expected, item)

    def test_delete(self):
        store = self.new_super_bowl_results_store()
        self.assertTrue(store.delete("lvi"))
        self.assertIsNone(store.read("lvi"))

    def test_delete_missing_key(self):
        store = self.new_super_bowl_results_store()
        self.assertFalse(store.delete("missing_key"))

    def test_count(self):
        store = self.new_super_bowl_results_store()
        self.assertEqual(56, store.count())
        filters = filter_factory(SuperBowlResult)
        self.assertEqual(
            20, store.count(filters.year.gte(1984) & filters.year.lt(2004))
        )
        self.assertEqual(6, store.count(filters.winner_code.contains("new_england")))
        self.assertEqual(0, store.count(AttrFilter("year", AttrFilterOp.lt, 1967)))

    def test_count_invalid_attr_filter(self):
        store = self.new_super_bowl_results_store()
        try:
            store.count(
                AttrFilter("non_attr", AttrFilterOp.gte, 1984)
            ) and self.assertTrue(False)
        except PersistyError:
            pass

    def test_count_exclude_all(self):
        store = self.new_super_bowl_results_store()
        self.assertEqual(0, store.count(EXCLUDE_ALL))

    def test_count_filter_id(self):
        store = self.new_number_name_store()
        filters = filter_factory(NumberName)
        num = store.count(
            filters.id.eq(UUID("00000000-0000-0000-0000-000000000010"))
            & filters.title.eq("Ten")
        )
        self.assertEqual(1, num)

    def test_count_custom_filter(self):
        @dataclass
        class StrLenFilter(SearchFilterABC[T]):
            attr_name: str
            required_length: int

            def lock_attrs(self, attrs: Tuple[Attr, ...]) -> SearchFilterABC[T]:
                attr = next(attr for attr in attrs if attr.name == self.attr_name)
                assert attr.readable
                return self

            def match(self, item: T, attrs: Tuple[Attr, ...]) -> bool:
                # noinspection PyTypeChecker
                return len(getattr(item, self.attr_name)) == self.required_length

        store = self.new_number_name_store()

        num = store.count(StrLenFilter("title", 4))
        self.assertEqual(3, num)  # Four, Five, Nine

    def test_count_non_indexed_filter(self):
        tag_store = self.new_number_name_store()
        filters = filter_factory(NumberName)
        count = tag_store.count(filters.title.eq("Five") & filters.value.eq(5))
        self.assertEqual(1, count)

    def test_search_all(self):
        store = self.new_super_bowl_results_store()
        self.assertEqual(SUPER_BOWL_RESULTS, list(store.search_all()))
        filters = filter_factory(SuperBowlResult)
        self.assertEqual(
            SUPER_BOWL_RESULTS[17:37],
            list(store.search_all(filters.year.gte(1984) & filters.year.lt(2004))),
        )
        self.assertEqual(
            [r for r in SUPER_BOWL_RESULTS if r.winner_code == "new_england"],
            list(
                store.search_all(
                    AttrFilter("winner_code", AttrFilterOp.contains, "new_england")
                )
            ),
        )
        self.assertEqual(
            [], list(store.search_all(AttrFilter("year", AttrFilterOp.lt, 1967)))
        )

    def test_search(self):
        store = self.new_number_name_store()
        filters = filter_factory(NumberName)
        search_filter = filters.value.lt(50)
        search_order = filters.value.asc()
        page1 = store.search(search_filter, search_order, None, 10)
        self.assertEqual(NUMBER_NAMES[:10], page1.results)
        page2 = store.search(search_filter, search_order, page1.next_page_key, 10)
        self.assertEqual(NUMBER_NAMES[10:20], page2.results)

    def test_search_all_id(self):
        store = self.new_number_name_store()
        filters = filter_factory(NumberName)
        id_ = "00000000-0000-0000-0000-000000000001"
        loaded = list(store.search_all(filters.id.eq(UUID(id_))))
        expected = [store.read(id_)]
        self.assertEqual(expected, loaded)

    def test_search_all_id_title(self):
        store = self.new_number_name_store()
        filters = filter_factory(NumberName)
        id_ = "00000000-0000-0000-0000-000000000001"
        loaded = list(
            store.search_all(filters.id.eq(UUID(id_)) & filters.title.contains("One"))
        )
        expected = [store.read(id_)]
        self.assertEqual(expected, loaded)

    def test_edit_all(self):
        store = self.new_number_name_store()
        edits = store.search_all(AttrFilter("value", AttrFilterOp.gt, 3))
        edits = (BatchEdit(delete_key=str(n.id)) for n in edits)
        list(store.edit_all(edits))
        self.assertEqual(3, store.count())
        edits = [
            BatchEdit(
                create_item=NumberName(
                    id=UUID("00000000-0000-0000-0001-000000000000"),
                    title="Minus One",
                    value=-1,
                )
            ),
            BatchEdit(
                update_item=NumberName(
                    id=UUID("00000000-0000-0000-0002-000000000001"),
                    title="Not existing",
                )
            ),
            BatchEdit(
                update_item=NumberName(
                    id=UUID("00000000-0000-0000-0000-000000000001"), title="First"
                )
            ),
            BatchEdit(delete_key="00000000-0000-0000-0002-000000000001"),
        ]
        now = datetime.now().astimezone(timezone.utc) + relativedelta(seconds=-2)
        results = [r.success for r in store.edit_all(edits)]
        self.assertEqual(results, [True, False, True, False])
        results = list(
            store.search_all(search_order=SearchOrder((SearchOrderAttr("value"),)))
        )
        results = sorted(results, key=lambda r: r.value)
        self.assertGreaterEqual(results[0].created_at, now)
        self.assertGreaterEqual(results[0].updated_at, now)
        self.assertLess(results[1].created_at, now)
        self.assertGreaterEqual(results[1].updated_at, now)
        expected_results = [
            NumberName(
                created_at=results[0].created_at,
                id=UUID("00000000-0000-0000-0001-000000000000"),
                title="Minus One",
                updated_at=results[0].updated_at,
                value=-1,
            ),
            NumberName(
                created_at=datetime.fromisoformat("1970-01-01T00:00:00+00:00"),
                id=UUID("00000000-0000-0000-0000-000000000001"),
                title="First",
                updated_at=results[1].updated_at,
                value=1,
            ),
            NumberName(
                created_at=datetime.fromisoformat("1970-01-01T00:00:00+00:00"),
                id=UUID("00000000-0000-0000-0000-000000000002"),
                title="Two",
                updated_at=datetime.fromisoformat("1970-01-01T00:00:00+00:00"),
                value=2,
            ),
            NumberName(
                created_at=datetime.fromisoformat("1970-01-01T00:00:00+00:00"),
                id=UUID("00000000-0000-0000-0000-000000000003"),
                title="Three",
                updated_at=datetime.fromisoformat("1970-01-01T00:00:00+00:00"),
                value=3,
            ),
        ]
        self.assertEqual(expected_results, results)

    def test_edit_batch(self):
        store = self.new_number_name_store()
        edits = [BatchEdit(delete_key=str(n.id)) for n in store.search().results]
        results = store.edit_batch(edits)
        for result in results:
            self.assertTrue(result.success)
        self.assertEqual(89, store.count())

    def test_update_no_key(self):
        store = self.new_number_name_store()
        with self.assertRaises(PersistyError):
            store.update(NumberName())

    def test_update_fail_filter(self):
        store = self.new_number_name_store()
        self.spec_for_update_fail_filter(store)

    def spec_for_update_fail_filter(self, store):
        item = store.update(
            NumberName(
                id=UUID("00000000-0000-0000-0000-000000000001"), title="Not One"
            ),
            filter_factory(NumberName).title.ne("One"),
        )
        self.assertIsNone(item)

    def test_search_custom_filter_full_result_set(self):
        store = self.new_number_name_store()
        search_filter = ValueLessThanFilter(21)
        page_1 = store.search(search_filter)
        self.assertEqual(list(range(1, 11)), list(i.value for i in page_1.results))
        page_2 = store.search(
            search_filter=search_filter, page_key=page_1.next_page_key
        )
        self.assertEqual(list(range(11, 21)), list(i.value for i in page_2.results))
        page_3 = store.search(
            search_filter=search_filter, page_key=page_2.next_page_key
        )
        self.assertEqual(ResultSet([]), page_3)

    def test_search_custom_filter_unfilled_result_set(self):
        store = self.new_number_name_store()
        limit = 3
        for less_than in range(1, 31):
            kwargs = dict(
                search_filter=ValueLessThanFilter(less_than),
                search_order=SearchOrder((SearchOrderAttr("value"),)),
                limit=limit,
            )
            index = 1
            while True:
                page = store.search(**kwargs)
                expected_values = [
                    v for v in range(index, min(less_than, index + limit))
                ]
                values = [r.value for r in page.results]
                self.assertEqual(expected_values, values)
                if page.next_page_key:
                    kwargs["page_key"] = page.next_page_key
                    index += limit
                else:
                    break

    def test_edit_batch_errors(self):
        store = self.new_number_name_store()
        edits = [
            BatchEdit(
                create_item=NumberName(
                    id=NUMBER_NAMES[1].id, value=-1, title="New Item"
                )
            ),
            BatchEdit(
                update_item=NumberName(id=uuid4(), value=-2, title="Updated Item")
            ),
            BatchEdit(delete_key=str(uuid4())),
            BatchEdit(),
        ]
        results = store.edit_batch(edits)
        self.assertFalse(next((True for r in results if r.success), False))

    def test_wrong_type_str(self):
        store = self.new_number_name_store()
        id = uuid4()
        item = NumberName(
            id=str(id),  # The type is wrong here, but we should handle it
            title="A Gazillion",
            value=-1,
        )
        store.create(item)
        # noinspection PyTypeChecker
        loaded = store.read(id)  # Again - type is wrong - this should be a str
        self.assertIsNotNone(loaded.created_at)
        self.assertIsNotNone(loaded.updated_at)
        item.id = id
        item.created_at = loaded.created_at
        item.updated_at = loaded.updated_at
        self.assertEqual(item, loaded)

        filters = filter_factory(NumberName)
        search_filter = filters.id.eq(str(id))
        results = list(store.search_all(search_filter))
        self.assertEqual([item], results)

        # noinspection PyTypeChecker
        self.assertTrue(
            store.delete(id)
        )  # Again - type is wrong - this should be a str
        # noinspection PyTypeChecker
        self.assertFalse(store.read(id))  # Again - type is wrong - this should be a str


@dataclass
class ValueLessThanFilter(SearchFilterABC[T]):
    """
    Custom filter for testing - in reality you would use a AttrFilter
    for this as store implementations would more easily be able to turn
    it into a native condition
    """

    value: int

    def lock_attrs(self, attrs: Tuple[Attr, ...]) -> SearchFilterABC[T]:
        return self

    def match(self, item: T, attrs: Tuple[Attr, ...]) -> bool:
        return item.value < self.value
