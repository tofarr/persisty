from unittest import TestCase

from persisty.storage.in_mem.in_mem_storage_context import InMemStorageContext
from persisty.storage.sql.table_storage import table_storage

import sqlite3

from persisty.storage.sql.close_wrapper import CloseWrapper
from persisty.storage.sql.sql_table import sql_table_from_type
from persisty.storage.wrappers.timestamped_storage import with_timestamps
from tests.fixtures.item_types import Band, Member, Node, NodeTag, Tag


class TestTableStorage(TestCase):

    def create_storage_context(self):
        storage_context = InMemStorageContext()
        connection = sqlite3.connect(':memory:')
        setattr(self, 'connection', connection)
        cursor = connection.cursor()
        for type_ in (Band, Member, Tag, Node, NodeTag):
            sql_table = sql_table_from_type(type_)
            create_sql = sql_table.create_table_sql()
            cursor.execute(create_sql)
            storage = with_timestamps(table_storage(self.cursor, type_))
            storage_context.register_storage(storage)
        return storage_context

    def cursor(self):
        connection = getattr(self, 'connection')
        cursor = connection.cursor()
        cursor = CloseWrapper(cursor)
        return cursor