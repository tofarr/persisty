from logging import Logger
from typing import Optional
from unittest import TestCase

from persisty.impl.mem.mem_store import mem_store
from persisty.obj_store.stored import get_meta
from persisty.store.logging_store import logging_store
from persisty.store.store_abc import StoreABC
from tests.fixtures.number_name import NumberName
from tests.util.test_logify import MockLogger


class TestLoggingStore(TestCase):
    @staticmethod
    def new_number_name_store(logger: Optional[Logger] = None) -> StoreABC:
        # noinspection PyTypeChecker
        store = mem_store(get_meta(NumberName))
        store = logging_store(store, logger)
        return store

    def test_create(self):
        logger = MockLogger()
        # noinspection PyTypeChecker
        store = self.new_number_name_store(logger)
        item = store.create(dict(title="One", value=1))
        read = store.read(item["id"])
        self.assertEqual(item, read)
        self.assertEqual(2, len(logger.infos))
        self.assertEqual("create", logger.infos[0]["name"])
        self.assertEqual("read", logger.infos[1]["name"])
        self.assertEqual("One", logger.infos[0]["args"][0]["title"])
        self.assertEqual(logger.infos[1]["args"][0], logger.infos[0]["args"][0]["id"])
