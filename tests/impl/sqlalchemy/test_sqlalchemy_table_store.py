from typing import Iterator
from unittest import TestCase

from marshy.types import ExternalItemType

from persisty.impl.sqlalchemy.sqlalchemy_context_factory import SqlalchemyContextFactory
from persisty.impl.sqlalchemy.sqlalchemy_table_store_factory import (
    SqlalchemyTableStoreFactory,
)
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta, get_meta
from tests.fixtures.author import AUTHOR_DICTS, Author
from tests.fixtures.book import Book, BOOK_DICTS
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
        factory = SqlalchemyTableStoreFactory(self.context, triggers=False)
        store = factory.create(store_meta)
        number_names = (
            {**r.__dict__, "result_date": r.result_date} for r in SUPER_BOWL_RESULTS
        )
        self.seed_table(store_meta, number_names)
        return store

    def new_number_name_store(self) -> StoreABC:
        store_meta = get_meta(NumberName)
        factory = SqlalchemyTableStoreFactory(self.context, triggers=False)
        store = factory.create(store_meta)
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

    def new_author_store(self) -> StoreABC:
        store_meta = get_meta(Author)
        factory = SqlalchemyTableStoreFactory(self.context, triggers=False)
        store = factory.create(store_meta)
        self.seed_table(store_meta, AUTHOR_DICTS)
        return store

    def new_book_store(self) -> StoreABC:
        store_meta = get_meta(Book)
        factory = SqlalchemyTableStoreFactory(self.context, triggers=False)
        store = factory.create(store_meta)
        self.seed_table(store_meta, BOOK_DICTS)
        return store

    def seed_table(self, store_meta: StoreMeta, items: Iterator[ExternalItemType]):
        table = self.context.get_table(store_meta)
        with self.context.engine.begin() as conn:
            stmt = table.insert()
            for item in items:
                conn.execute(stmt, item)
            conn.commit()
