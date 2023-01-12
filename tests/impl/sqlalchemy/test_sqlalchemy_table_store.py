from typing import Iterator
from unittest import TestCase

from marshy.types import ExternalItemType

from persisty.impl.sqlalchemy.sqlalchemy_context_factory import SqlalchemyContextFactory
from persisty.impl.sqlalchemy.sqlalchemy_table_store_factory import (
    SqlalchemyTableStoreFactory,
)
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta, get_meta
from tests.fixtures.number_name import NumberName, NUMBER_NAMES
from tests.fixtures.storage_tst_abc import StoreTstABC
from tests.fixtures.super_bowl_results import (
    SuperBowlResult,
    SUPER_BOWL_RESULTS,
)


class TestSqlalchemyTableStore(TestCase, StoreTstABC):
    def setUp(self) -> None:
        self.context = SqlalchemyContextFactory().create()

    def tearDown(self) -> None:
        pass

    def new_super_bowl_results_store(self) -> StoreABC:
        store_meta = get_meta(SuperBowlResult)
        factory = SqlalchemyTableStoreFactory(store_meta, self.context)
        store = factory.create()
        number_names = ({**r.__dict__, "date": r.date} for r in SUPER_BOWL_RESULTS)
        self.seed_table(store_meta, number_names)
        return store

    def new_number_name_store(self) -> StoreABC:
        store_meta = get_meta(NumberName)
        factory = SqlalchemyTableStoreFactory(store_meta, self.context)
        store = factory.create()
        number_names = (
            {
                **r.__dict__,
                "id": str(r.id),
                "created_at": r.created_at,
                "updated_at": r.updated_at,
            }
            for r in NUMBER_NAMES
        )
        self.seed_table(store_meta, number_names)
        return store

    def seed_table(self, store_meta: StoreMeta, items: Iterator[ExternalItemType]):
        table = self.context.get_table(store_meta)
        with self.context.engine.begin() as conn:
            stmt = table.insert()
            for item in items:
                conn.execute(stmt, item)
            conn.commit()

    """
    def test_search_all_sorted(self):
        store = self.new_super_bowl_results_store()
        filters = filter_factory(SuperBowlResult)
        self.assertEqual(
            list(reversed(SUPER_BOWL_RESULT_DICTS)),
            list(store.search_all(INCLUDE_ALL, filters.year.desc())),
        )
        self.assertEqual(
            list(reversed(SUPER_BOWL_RESULT_DICTS[17:37])),
            list(
                store.search_all(
                    filters.year.gte(1984) & filters.year.lt(2004), filters.year.desc()
                )
            ),
        )

    def test_mem_store_no_dict(self):
        store = mem_store(get_meta(NumberName))
        created = store.create(dict(value=1, title="One"))
        loaded = store.read(created["id"])
        self.assertEqual(loaded, created)

    def test_mem_store_secured(self):
        store_meta = StoreMeta(
            name="read_only_number_name",
            attrs=get_meta(NumberName).attrs,
            access_control=READ_ONLY,
        )
        store = mem_store(store_meta)
        store.read("1")
        error = None
        try:
            store.create(dict(value=-1, title="Minus One"))
        except PersistyError as e:
            error = e
        self.assertIsNotNone(error)

    def test_mem_store_delete_missing_key(self):
        store = MemStore(get_meta(NumberName))
        self.assertFalse(store.delete("missing_key"))

    def test_mem_store_update_missing_key(self):
        # noinspection PyUnresolvedReferences
        store = self.new_super_bowl_results_store().store
        self.spec_for_update_missing_key(store)

    def test_mem_store_update_fail_filter(self):
        # noinspection PyUnresolvedReferences
        store = self.new_number_name_store().store
        self.spec_for_update_fail_filter(store)

    def test_mem_store_create_missing_key(self):
        store_meta = get_meta(NumberName)
        store_meta = StoreMeta(
            name=store_meta.name,
            attrs=tuple(
                dataclasses.replace(
                    f, write_transform=None, is_creatable=True, is_updatable=True
                )
                for f in store_meta.attrs
            ),
        )
        store = MemStore(store_meta)
        try:
            store.create({}) and self.assertTrue(False)
        except PersistyError:
            self.assertEqual(0, store.count())

    def test_mem_store_update_no_key(self):
        store_meta = get_meta(NumberName)
        store_meta = StoreMeta(
            name=store_meta.name,
            attrs=tuple(
                dataclasses.replace(
                    f, write_transform=None, is_creatable=True, is_updatable=True
                )
                for f in store_meta.attrs
            ),
        )
        store = MemStore(store_meta)
        try:
            store.update(dict(title="foobar")) and self.assertTrue(False)
        except PersistyError:
            self.assertEqual(0, store.count())
    """
