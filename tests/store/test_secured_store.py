import dataclasses
from unittest import TestCase

from marshy import dump

from persisty.access_control.constants import NO_ACCESS
from persisty.impl.mem.mem_store import mem_store, MemStore
from persisty.obj_store.stored import get_meta
from persisty.store.secured_store import SecuredStore
from persisty.store.store_abc import StoreABC
from tests.fixtures.number_name import NUMBER_NAMES, NumberName


class TestSecuredStore(TestCase):
    @staticmethod
    def new_number_name_store() -> StoreABC:
        number_names = (dump(r) for r in NUMBER_NAMES)
        # noinspection PyTypeChecker
        store = MemStore(
            get_meta(NumberName),
            {r["id"]: r for r in number_names},
        )
        return store

    def test_search_with_no_access(self):
        store = self.new_number_name_store()
        store = SecuredStore(
            store,
            dataclasses.replace(store.get_meta(), access_control=NO_ACCESS),
        )
        self.assertEqual(0, store.count())
