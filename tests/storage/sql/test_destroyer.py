import sqlite3
from unittest import TestCase

from persisty.errors import PersistyError
from persisty.storage.sql.destroyer import Destroyer


class TestDestroyer(TestCase):

    def test_destroy_exception(self):
        connection = sqlite3.connect(':memory:')
        cursor = connection.cursor()
        destroyer = Destroyer('NOT VALID SQL')
        with self.assertRaises(PersistyError):
            destroyer.destroy(cursor, 'foobar')