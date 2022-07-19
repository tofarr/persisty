from abc import ABC, abstractmethod
from unittest import TestCase

from persisty.storage.storage_abc import StorageABC


class StorageTestABC(TestCase, ABC):
    """ Tests which expect storage to have the bands data loaded """

    @abstractmethod
    def new_super_bowl_results_storage(self) -> StorageABC:
        """ Create a new storage object containing only BANDS """

    def test_read(self):
        storage = self.new_super_bowl_results_storage()
        found = storage.read('iii')
        expected = {'code': 'iii', 'year': 1969, 'date': '1969-01-12T00:00:00', 'winner_code': 'new_york_jets',
                    'runner_up_code': 'baltimore', 'winner_score': 16, 'runner_up_score': 7}
        self.assertEqual(expected, found)

    def test_read_not_existing(self):
        storage = self.new_super_bowl_results_storage()
        found = storage.read('not_a_code')
        self.assertIsNone(found)
