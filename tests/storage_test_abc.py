from abc import ABC, abstractmethod
from copy import deepcopy
from unittest import TestCase

from persisty.errors import PersistyError
from persisty.storage.field.field_filter import FieldFilter, FieldFilterOp
from persisty.storage.storage_abc import StorageABC


class StorageTestABC(TestCase, ABC):
    """Tests which expect storage to have the bands data loaded"""

    @abstractmethod
    def new_super_bowl_results_storage(self) -> StorageABC:
        """Create a new storage object containing only BANDS"""

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
        read = storage.read('c')
        self.assertEqual(item, read)
        self.assertEqual(57, storage.count())

    def test_create_invalid_schema(self):
        storage = self.new_super_bowl_results_storage()
        item = {
            "code": "c",
            "year": 'not_a_year',
            "date": "2067-01-15T00:00:00",
            "winner_code": "robots",
            "runner_up_code": "humans",
            "winner_score": 1234,
            "runner_up_score": 0,
        }
        try:
            storage.create(deepcopy(item))
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
            storage.create(deepcopy(item))
        except PersistyError:
            self.assertEqual(56, storage.count())

    def test_update(self):
        storage = self.new_super_bowl_results_storage()
        item = {
            "code": "li",
            "winner_code": "tom_brady_fan_club",
        }
        storage.update(item)
        item = storage.read('li')
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

    def test_update_missing_key(self):
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
        updates = {**item}
        try:
            storage.update(updates)
        except PersistyError:
            self.assertEqual(56, storage.count())
        self.assertEqual(item, updates)

    def test_update_invalid_schema(self):
        storage = self.new_super_bowl_results_storage()
        try:
            storage.update({
                "code": "i",
                "date": "not_a_date"
            })
        except PersistyError:
            self.assertEqual(56, storage.count())
        read = storage.read('i')
        expected = {
            "code": "i",
            "year": 1967,
            "date": "1967-01-15T00:00:00",
            "winner_code": "green_bay",
            "runner_up_code": "kansas_city",
            "winner_score": 35,
            "runner_up_score": 10,
        }
        self.assertEqual(expected, read)

    def test_update_valid_filter(self):
        storage = self.new_super_bowl_results_storage()
        item = {
            "code": "li",
            "winner_code": "tom_brady_fan_club",
        }
        storage.update(item, FieldFilter('year', FieldFilterOp.eq, 2017))
        item = storage.read('li')
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
        storage.update(item, FieldFilter('year', FieldFilterOp.eq, 2018))
        item = storage.read('li')
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
