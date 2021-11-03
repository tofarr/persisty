from persisty import get_persisty_context
from persisty.sql.close_wrapper import CloseWrapper
from persisty.sql.sql_table import sql_table_from_type
from persisty.sql.table_store import table_store
from persisty.store_schemas import schemas_for_type
from tests.fixtures.data import setup_bands
from tests.fixtures.items import Band, Issue

import sqlite3

from tests.store.test_in_mem_store import TestInMemStore


class TestTableStore(TestInMemStore):

    def setUp(self):
        connection = sqlite3.connect(':memory:')
        setattr(self, 'connection', connection)
        cursor = connection.cursor()
        sql_table = sql_table_from_type(Band)
        create_sql = sql_table.create_table_sql()
        cursor.execute(create_sql)
        store = table_store(self.cursor, Band, sql_table)
        persisty_context = get_persisty_context()
        setup_bands(store)
        persisty_context.register_store(store)

    def cursor(self):
        connection = getattr(self, 'connection')
        cursor = connection.cursor()
        cursor = CloseWrapper(cursor)
        return cursor

    def test_get_schemas(self):
        store = self.get_band_store()
        expected = schemas_for_type(Band)
        assert store.schemas == expected

    def test_generating_keys(self):
        sql_table = sql_table_from_type(Issue)
        create_sql = sql_table.create_table_sql()
        with self.cursor() as cursor:
            cursor.execute(create_sql)
        store = table_store(self.cursor, Issue, sql_table)
        issue = Issue('Issue 1')
        key = store.create(issue)
        assert store.read(key) == issue
        issue.title = 'Issue 1 Updated'
        store.update(issue)
        assert store.read(key) == issue
        store.destroy(key)
        assert store.read(key) is None
