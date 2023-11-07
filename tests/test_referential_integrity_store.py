import dataclasses
from unittest import TestCase
from unittest.mock import patch

from persisty.batch_edit import BatchEdit
from persisty.errors import PersistyError
from persisty.impl.mem.mem_store_factory import MemStoreFactory
from persisty.link.on_delete import OnDelete
from persisty.search_filter.filter_factory import filter_factory
from persisty.store import referential_integrity_store
from persisty.store.store_abc import StoreABC
from persisty.store_meta import get_meta
from tests.fixtures.author import Author, AUTHORS
from tests.fixtures.book import Book, BOOKS


def create_store_meta(on_delete: OnDelete):
    author_store_meta = get_meta(Author)
    book_store_meta = get_meta(Book)
    author_store_meta = dataclasses.replace(
        author_store_meta,
        store_factory=MemStoreFactory(
            {str(a.id): dataclasses.replace(a) for a in AUTHORS}, False, True
        ),
    )
    # noinspection PyDataclass
    book_store_meta = dataclasses.replace(
        book_store_meta,
        store_factory=MemStoreFactory(
            {str(b.id): dataclasses.replace(b) for b in BOOKS}, False, True
        ),
        links=(
            dataclasses.replace(
                book_store_meta.links[0],
                linked_store_meta=author_store_meta,
                on_delete=on_delete,
            ),
        ),
    )
    return [author_store_meta, book_store_meta]


def _patch_it(on_delete: OnDelete):
    store_metas = create_store_meta(on_delete)
    return patch(
        "persisty.store.referential_integrity_store.find_store_meta",
        lambda: iter(store_metas),
    )


class TestReferentialIntegrityStore(TestCase):
    def test_block(self):
        with _patch_it(OnDelete.BLOCK):
            author_store_meta, book_store_meta = list(
                referential_integrity_store.find_store_meta()
            )
            author_store: StoreABC = author_store_meta.create_store()
            with self.assertRaises(PersistyError):
                author_store.delete("1")
            book_store: StoreABC = book_store_meta.create_store()
            books = list(book_store.search_all(filter_factory(Book).author_id.eq("1")))
            list(book_store.edit_all((BatchEdit(delete_key=str(b.id))) for b in books))
            author_store.delete("1")

    def test_nullify(self):
        with _patch_it(OnDelete.NULLIFY):
            author_store_meta, book_store_meta = list(
                referential_integrity_store.find_store_meta()
            )
            author_store: StoreABC = author_store_meta.create_store()
            author_store.delete("1")
            book_store: StoreABC = book_store_meta.create_store()
            books = list(book_store.search_all())
            self.assertEqual(sum(1 for book in books if book.author_id is None), 2)
            self.assertEqual(sum(1 for book in books if book.author_id == "1"), 0)

    def test_cascade(self):
        with _patch_it(OnDelete.CASCADE):
            author_store_meta, book_store_meta = list(
                referential_integrity_store.find_store_meta()
            )
            author_store: StoreABC = author_store_meta.create_store()
            author_store.delete("1")
            book_store: StoreABC = book_store_meta.create_store()
            books = list(book_store.search_all())
            self.assertEqual(sum(1 for book in books if book.author_id is None), 0)
            self.assertEqual(sum(1 for book in books if book.author_id == "1"), 0)
            self.assertEqual(len(books), 6)
