import dataclasses
from unittest import TestCase

from marshy import dump

from persisty.access_control.constants import NO_ACCESS
from persisty.impl.mem.mem_storage import mem_storage, MemStorage
from persisty.obj_storage.stored import get_storage_meta
from persisty.storage.secured_storage import SecuredStorage
from persisty.storage.storage_abc import StorageABC
from tests.fixtures.number_name import NUMBER_NAMES, NumberName


class TestSecuredStorage(TestCase):
    @staticmethod
    def new_number_name_storage() -> StorageABC:
        number_names = (dump(r) for r in NUMBER_NAMES)
        # noinspection PyTypeChecker
        storage = MemStorage(
            get_storage_meta(NumberName),
            {r["id"]: r for r in number_names},
        )
        return storage

    def test_search_with_no_access(self):
        storage = self.new_number_name_storage()
        storage = SecuredStorage(
            storage,
            dataclasses.replace(storage.get_storage_meta(), access_control=NO_ACCESS),
        )
        self.assertEqual(0, storage.count())
