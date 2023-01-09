from typing import Iterator
from unittest import TestCase

from marshy.types import ExternalItemType
from servey.security.authorization import ROOT

from persisty.impl.sqlalchemy.sqlalchemy_context_factory import SqlalchemyContextFactory
from persisty.impl.sqlalchemy.sqlalchemy_table_storage_factory import (
    SqlalchemyTableStorageFactory,
)
from persisty.obj_storage.stored import get_storage_meta
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta
from tests.fixtures.number_name import NumberName, NUMBER_NAMES
from tests.fixtures.super_bowl_results import (
    SuperBowlResult,
    SUPER_BOWL_RESULTS,
)
from tests.fixtures.storage_tst_abc import StorageTstABC


class TestSqlalchemyTableStorage(TestCase, StorageTstABC):
    def setUp(self) -> None:
        self.context = SqlalchemyContextFactory().create()

    def tearDown(self) -> None:
        pass

    def new_super_bowl_results_storage(self) -> StorageABC:
        storage_meta = get_storage_meta(SuperBowlResult)
        factory = SqlalchemyTableStorageFactory(storage_meta, self.context)
        storage = factory.create(ROOT)
        number_names = ({**r.__dict__, "date": r.date} for r in SUPER_BOWL_RESULTS)
        self.seed_table(storage_meta, number_names)
        return storage

    def new_number_name_storage(self) -> StorageABC:
        storage_meta = get_storage_meta(NumberName)
        factory = SqlalchemyTableStorageFactory(storage_meta, self.context)
        storage = factory.create(ROOT)
        number_names = (
            {
                **r.__dict__,
                "id": str(r.id),
                "created_at": r.created_at,
                "updated_at": r.updated_at,
            }
            for r in NUMBER_NAMES
        )
        self.seed_table(storage_meta, number_names)
        return storage

    def seed_table(self, storage_meta: StorageMeta, items: Iterator[ExternalItemType]):
        table = self.context.get_table(storage_meta)
        with self.context.engine.begin() as conn:
            stmt = table.insert()
            for item in items:
                conn.execute(stmt, item)
            conn.commit()

    """
    def test_search_all_sorted(self):
        storage = self.new_super_bowl_results_storage()
        filters = filter_factory(SuperBowlResult)
        self.assertEqual(
            list(reversed(SUPER_BOWL_RESULT_DICTS)),
            list(storage.search_all(INCLUDE_ALL, filters.year.desc())),
        )
        self.assertEqual(
            list(reversed(SUPER_BOWL_RESULT_DICTS[17:37])),
            list(
                storage.search_all(
                    filters.year.gte(1984) & filters.year.lt(2004), filters.year.desc()
                )
            ),
        )

    def test_mem_storage_no_dict(self):
        storage = mem_storage(get_storage_meta(NumberName))
        created = storage.create(dict(value=1, title="One"))
        loaded = storage.read(created["id"])
        self.assertEqual(loaded, created)

    def test_mem_storage_secured(self):
        storage_meta = StorageMeta(
            name="read_only_number_name",
            fields=get_storage_meta(NumberName).fields,
            access_control=READ_ONLY,
        )
        storage = mem_storage(storage_meta)
        storage.read("1")
        error = None
        try:
            storage.create(dict(value=-1, title="Minus One"))
        except PersistyError as e:
            error = e
        self.assertIsNotNone(error)

    def test_mem_storage_delete_missing_key(self):
        storage = MemStorage(get_storage_meta(NumberName))
        self.assertFalse(storage.delete("missing_key"))

    def test_mem_storage_update_missing_key(self):
        # noinspection PyUnresolvedReferences
        storage = self.new_super_bowl_results_storage().storage
        self.spec_for_update_missing_key(storage)

    def test_mem_storage_update_fail_filter(self):
        # noinspection PyUnresolvedReferences
        storage = self.new_number_name_storage().storage
        self.spec_for_update_fail_filter(storage)

    def test_mem_storage_create_missing_key(self):
        storage_meta = get_storage_meta(NumberName)
        storage_meta = StorageMeta(
            name=storage_meta.name,
            fields=tuple(
                dataclasses.replace(
                    f, write_transform=None, is_creatable=True, is_updatable=True
                )
                for f in storage_meta.fields
            ),
        )
        storage = MemStorage(storage_meta)
        try:
            storage.create({}) and self.assertTrue(False)
        except PersistyError:
            self.assertEqual(0, storage.count())

    def test_mem_storage_update_no_key(self):
        storage_meta = get_storage_meta(NumberName)
        storage_meta = StorageMeta(
            name=storage_meta.name,
            fields=tuple(
                dataclasses.replace(
                    f, write_transform=None, is_creatable=True, is_updatable=True
                )
                for f in storage_meta.fields
            ),
        )
        storage = MemStorage(storage_meta)
        try:
            storage.update(dict(title="foobar")) and self.assertTrue(False)
        except PersistyError:
            self.assertEqual(0, storage.count())
    """
