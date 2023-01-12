from unittest import TestCase
from uuid import UUID, uuid4

from dataclasses import dataclass, attr

from persisty.errors import PersistyError
from persisty.impl.mem.mem_store import MemStore
from persisty.key_config.attr_key_config import FIELD_KEY_CONFIG
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.obj_store.stored import get_meta
from persisty.store.batch_edit import BatchEditABC, Delete
from persisty.store.batch_edit_result import BatchEditResult
from persisty.attr.attr_filter import FieldFilterOp, FieldFilter
from persisty.store.result_set import ResultSet
from persisty.store.store_abc import skip_to_page, edit_batch
from tests.fixtures.number_name import NumberName
from tests.fixtures.super_bowl_results import SUPER_BOWL_RESULT_DICTS, SuperBowlResult


class TestBaseStore(TestCase):
    def test_skip_to_page(self):
        items = (dict(id=n) for n in range(10))
        skip_to_page("4", items, FIELD_KEY_CONFIG)
        self.assertEqual(dict(id=5), next(items))
        with self.assertRaises(PersistyError):
            skip_to_page("-1", items, FIELD_KEY_CONFIG)

    def test_edit_batch_unknown_type(self):
        @dataclass
        class Mutation(BatchEditABC):
            id: UUID = attr(default_factory=uuid4)

            def get_key(self, key_config: KeyConfigABC) -> str:
                """Not required"""

            def get_id(self) -> UUID:
                """Not required"""

        store = MemStore(get_meta(NumberName))
        edits = [Mutation()]
        results = edit_batch(store, edits)
        self.assertEqual(
            [BatchEditResult(edits[0], False, "unsupported_edit_type", "Mutation")],
            results,
        )

    def test_edit_batch_exception(self):
        class ErrorMemStore(MemStore):
            def delete(self, key: str):
                raise PersistyError("i_dont_like_you")

        store = ErrorMemStore(get_meta(NumberName))
        edits = [Delete("foobar")]
        results = edit_batch(store, edits)
        self.assertEqual(
            [BatchEditResult(edits[0], False, "exception", "i_dont_like_you")], results
        )

    def test_search_no_limit(self):
        store = MemStore(
            get_meta(SuperBowlResult),
            {n["code"]: n for n in SUPER_BOWL_RESULT_DICTS},
        )
        result_set = store.search(FieldFilter("year", FieldFilterOp.lte, 1967))
        expected = ResultSet(
            results=[
                {
                    "code": "i",
                    "year": 1967,
                    "date": "1967-01-15T00:00:00",
                    "winner_code": "green_bay",
                    "runner_up_code": "kansas_city",
                    "winner_score": 35,
                    "runner_up_score": 10,
                }
            ]
        )
        self.assertEqual(expected, result_set)
