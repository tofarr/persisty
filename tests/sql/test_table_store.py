from old.persisty.persisty_context import get_default_persisty_context
from persisty.storage.sql import CloseWrapper
from persisty.storage.sql import sql_table_from_type
from persisty.storage.sql import table_storage
from old.persisty.storage_schemas import schemas_for_type
from tests.fixtures.data import setup_bands
from tests.fixtures.items import Band, Issue

import sqlite3

from tests import TestInMemStorage


class TestTableStorage(TestInMemStorage):

    def setUp(self):
        connection = sqlite3.connect(':memory:')
        setattr(self, 'connection', connection)
        cursor = connection.cursor()
        sql_table = sql_table_from_type(Band)
        create_sql = sql_table.create_table_sql()
        cursor.execute(create_sql)
        storage = table_storage(self.cursor, Band, sql_table)
        persisty_context = get_default_persisty_context()
        setup_bands(storage)
        persisty_context.register_storage(storage)

    def cursor(self):
        connection = getattr(self, 'connection')
        cursor = connection.cursor()
        cursor = CloseWrapper(cursor)
        return cursor

    def test_get_schemas(self):
        storage = self.get_band_storage()
        expected = schemas_for_type(Band)
        assert storage.schemas == expected

    def test_generating_keys(self):
        sql_table = sql_table_from_type(Issue)
        create_sql = sql_table.create_table_sql()
        with self.cursor() as cursor:
            cursor.execute(create_sql)
        storage = table_storage(self.cursor, Issue, sql_table)
        issue = Issue('Issue 1')
        key = storage.create(issue)
        assert storage.read(key) == issue
        issue.title = 'Issue 1 Updated'
        storage.update(issue)
        assert storage.read(key) == issue
        storage.destroy(key)
        assert storage.read(key) is None
