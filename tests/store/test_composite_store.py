import dataclasses
from unittest import TestCase

from persisty.errors import PersistyError
from persisty.impl.mem.mem_store import mem_store
from persisty.obj_store.filter_factory import filter_factory
from persisty.obj_store.stored import stored, get_meta
from persisty.search_filter.not_filter import Not
from persisty.store.composite_store import CompositeStore


class TestCompositeStore(TestCase):
    @staticmethod
    def create_composite_store():
        store_meta = get_meta(CompositeItem)
        # Resetting the sequence value feels weird here
        # maybe the value should be deep copied for each new store object?
        # noinspection PyUnresolvedReferences
        store_meta.attrs[0].write_transform.value = 1
        store = CompositeStore(
            store=tuple(
                mem_store(dataclasses.replace(store_meta, name=n))
                for n in ("a", "b")
            ),
        )
        for i in range(1, 101):
            s = store.store[i % 2]
            s.create(dict(title=f"Item {i}"))
        return store

    def test_create_no_key(self):
        store = self.create_composite_store()
        item = store.create(dict(title="No Key"))
        self.assertEqual(dict(id="a/101", title="No Key"), item)
        item = store.read("a/101")
        self.assertEqual(dict(id="a/101", title="No Key"), item)
        self.assertEqual(101, store.count())

    def test_create_with_key(self):
        store = self.create_composite_store()
        item = store.create(dict(id="b/101", title="B Key"))
        self.assertEqual(dict(id="b/101", title="B Key"), item)
        item = store.read("b/101")
        self.assertEqual(dict(id="b/101", title="B Key"), item)
        self.assertEqual(101, store.count())

    def test_create_with_invalid_key(self):
        store = self.create_composite_store()
        error = None
        try:
            store.create(dict(id="c/101", title="C Key"))
        except PersistyError as e:
            error = e
            self.assertEqual(100, store.count())
        self.assertIsNotNone(error)

    def test_read_with_missing_key(self):
        store = self.create_composite_store()
        self.assertIsNone(store.read("b/101"))

    def test_read_with_invalid_key(self):
        store = self.create_composite_store()
        error = None
        try:
            store.read("c/55")
        except PersistyError as e:
            error = e
        self.assertIsNotNone(error)

    def test_update(self):
        store = self.create_composite_store()
        updates = dict(id="b/3", title="Updated Item")
        item = store.update(updates)
        self.assertIsNot(updates, item)
        self.assertEqual(updates, item)
        read = store.read("b/3")
        self.assertEqual(read, item)
        self.assertEqual(100, store.count())

    def test_update_with_missing_key(self):
        store = self.create_composite_store()
        self.assertIsNone(store.update(dict(id="b/101", title="Updated Item")))
        self.assertEqual(100, store.count())

    def test_update_with_invalid_key(self):
        store = self.create_composite_store()
        error = None
        try:
            store.update(dict(id="c/55", title="Updated Item"))
        except PersistyError as e:
            error = e
        self.assertIsNotNone(error)
        self.assertEqual(100, store.count())

    def test_delete(self):
        store = self.create_composite_store()
        deleted = store.delete("b/3")
        self.assertTrue(deleted)
        self.assertEqual(99, store.count())
        deleted = store.delete("b/3")
        self.assertFalse(deleted)
        self.assertEqual(99, store.count())

    def test_delete_with_invalid_key(self):
        store = self.create_composite_store()
        error = None
        try:
            store.delete("c/55")
        except PersistyError as e:
            error = e
        self.assertIsNotNone(error)

    def test_count_with_search_filter(self):
        store = self.create_composite_store()
        search_filter = filter_factory(CompositeItem).title.gt("Item 50")
        count = store.count(search_filter)
        self.assertEqual(53, count)

    def test_count_with_search_filter_on_key(self):
        store = self.create_composite_store()
        search_filter = filter_factory(CompositeItem).id.gt("b/53")
        count = store.count(search_filter)
        self.assertEqual(25, count)

    def test_search_all_with_search_filter(self):
        store = self.create_composite_store()
        search_filter = filter_factory(CompositeItem).title.gt("Item 94")
        results = {i["id"]: i["title"] for i in store.search_all(search_filter)}
        expected = {
            ("a", "b")[i % 2] + "/" + str(i): f"Item {i}" for i in range(95, 100)
        }
        self.assertEqual(expected, results)

    def test_search_with_search_filter_on_key(self):
        store = self.create_composite_store()
        search_filter = filter_factory(CompositeItem).id.gt("b/90")
        results = {i["id"]: i["title"] for i in store.search_all(search_filter)}
        expected = {f"b/{i}": f"Item {i}" for i in range(91, 101, 2)}
        self.assertEqual(expected, results)

    def test_search_with_search_filter_on_key_and_sort_order(self):
        store = self.create_composite_store()
        filters = filter_factory(CompositeItem)
        search_filter = filters.title.gt("Item 94")
        results = list(store.search_all(search_filter, filters.title.desc()))
        expected = [
            dict(id=("a", "b")[i % 2] + "/" + str(i), title=f"Item {i}")
            for i in range(99, 94, -1)
        ]
        self.assertEqual(expected, results)

    def test_count_composite_filter(self):
        store = self.create_composite_store()
        filters = filter_factory(CompositeItem)
        search_filter = Not(filters.title.gt("Item 94") & filters.title.lt("Item 98"))
        count = store.count(search_filter)
        self.assertEqual(97, count)


@stored
class CompositeItem:
    id: str
    title: str
