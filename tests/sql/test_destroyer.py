import sqlite3

from persisty.errors import PersistyError
from persisty.sql.destroyer import Destroyer
from tests.store.test_in_mem_store import TestInMemStore


class TestTableStore(TestInMemStore):

    def test_destroy_exception(self):
        connection = sqlite3.connect(':memory:')
        cursor = connection.cursor()
        destroyer = Destroyer('NOT VALID SQL')
        with self.assertRaises(PersistyError):
            destroyer.destroy(cursor, 'foobar')