import dataclasses
from unittest import TestCase

from persisty.errors import PersistyError
from persisty.impl.mem.mem_store import MemStore
from persisty.impl.mem.mem_store_factory import MemStoreFactory
from persisty.search_filter.filter_factory import filter_factory
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.store.store_abc import StoreABC
from persisty.store_meta import get_meta, StoreMeta
from tests.fixtures.number_name import NumberName, NUMBER_NAMES
from tests.fixtures.storage_tst_abc import StoreTstABC
from tests.fixtures.super_bowl_results import (
    SuperBowlResult,
    SUPER_BOWL_RESULTS,
)


class TestMemStore(TestCase, StoreTstABC):
    def new_super_bowl_results_store(self) -> StoreABC:
        factory = MemStoreFactory(
            get_meta(SuperBowlResult),
            {r.code: dataclasses.replace(r) for r in SUPER_BOWL_RESULTS},
        )
        return factory.create()

    def new_number_name_store(self) -> StoreABC:
        # noinspection PyTypeChecker
        factory = MemStoreFactory(
            get_meta(NumberName),
            {str(r.id): dataclasses.replace(r) for r in NUMBER_NAMES},
        )
        return factory.create()

    def test_search_all_sorted(self):
        store = self.new_super_bowl_results_store()
        filters = filter_factory(SuperBowlResult)
        self.assertEqual(
            list(reversed(SUPER_BOWL_RESULTS)),
            list(store.search_all(INCLUDE_ALL, filters.year.desc())),
        )
        self.assertEqual(
            list(reversed(SUPER_BOWL_RESULTS[17:37])),
            list(
                store.search_all(
                    filters.year.gte(1984) & filters.year.lt(2004), filters.year.desc()
                )
            ),
        )

    def test_mem_store_no_dict(self):
        store = MemStoreFactory(get_meta(NumberName)).create()
        created = store.create(NumberName(value=1, title="One"))
        loaded = store.read(str(created.id))
        self.assertEqual(loaded, created)

    def test_mem_store_delete_missing_key(self):
        store = MemStore(get_meta(NumberName))
        self.assertFalse(store.delete("missing_key"))

    def test_mem_store_create_missing_key(self):
        store_meta = get_meta(NumberName)
        store_meta = StoreMeta(
            name=store_meta.name,
            attrs=tuple(
                dataclasses.replace(
                    f,
                    create_generator=None,
                    update_generator=None,
                    creatable=True,
                    updatable=True,
                )
                for f in store_meta.attrs
            ),
        )
        store = MemStore(store_meta)
        with self.assertRaises(PersistyError):
            store.create(NumberName())
        self.assertEqual(0, store.count())

    def test_mem_store_update_no_key(self):
        store_meta = get_meta(NumberName)
        store_meta = StoreMeta(
            name=store_meta.name,
            attrs=tuple(
                dataclasses.replace(
                    f,
                    create_generator=None,
                    update_generator=None,
                    creatable=True,
                    updatable=True,
                )
                for f in store_meta.attrs
            ),
        )
        store = MemStore(store_meta)
        with self.assertRaises(PersistyError):
            store.update(NumberName(title="foobar"))
        self.assertEqual(0, store.count())
