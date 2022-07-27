from logging import Logger
from typing import Optional
from unittest import TestCase

from persisty.impl.mem.mem_storage import mem_storage
from persisty.obj_storage.stored import get_storage_meta
from persisty.storage.logging_storage import logging_storage
from persisty.storage.storage_abc import StorageABC
from tests.fixtures.number_name import NumberName
from tests.util.test_logify import MockLogger


class TestLoggingStorage(TestCase):

    @staticmethod
    def new_number_name_storage(logger: Optional[Logger] = None) -> StorageABC:
        # noinspection PyTypeChecker
        storage = mem_storage(get_storage_meta(NumberName))
        storage = logging_storage(storage, logger)
        return storage

    def test_create(self):
        logger = MockLogger()
        # noinspection PyTypeChecker
        storage = self.new_number_name_storage(logger)
        item = storage.create(dict(title="One", value=1))
        read = storage.read(item["id"])
        self.assertEqual(item, read)
        self.assertEqual(2, len(logger.infos))
        self.assertEqual("create", logger.infos[0]['name'])
        self.assertEqual("read", logger.infos[1]['name'])
        self.assertEqual("One", logger.infos[0]['args'][0]['title'])
        self.assertEqual(logger.infos[1]['args'][0], logger.infos[0]['args'][0]['id'])
