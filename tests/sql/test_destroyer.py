import sqlite3

from old.persisty import PersistyError
from persisty.storage.sql import Destroyer
from tests import TestInMemStorage


class TestDestroyer(TestInMemStorage):

    def test_destroy_exception(self):
        connection = sqlite3.connect(':memory:')
        cursor = connection.cursor()
        destroyer = Destroyer('NOT VALID SQL')
        with self.assertRaises(PersistyError):
            destroyer.destroy(cursor, 'foobar')