import dataclasses
from unittest import TestCase

from persisty.errors import PersistyError
from persisty.impl.mem.mem_storage import mem_storage
from persisty.obj_storage.filter_factory import filter_factory
from persisty.obj_storage.stored import stored, get_storage_meta
from persisty.search_filter.not_filter import Not
from persisty.storage.composite_storage import CompositeStorage


class TestCompositeStorage(TestCase):
    @staticmethod
    def create_composite_storage():
        storage_meta = get_storage_meta(CompositeItem)
        # Resetting the sequence value feels weird here
        # maybe the value should be deep copied for each new storage object?
        # noinspection PyUnresolvedReferences
        storage_meta.fields[0].write_transform.value = 1
        storage = CompositeStorage(
            storage=tuple(
                mem_storage(dataclasses.replace(storage_meta, name=n))
                for n in ("a", "b")
            ),
        )
        for i in range(1, 101):
            s = storage.storage[i % 2]
            s.create(dict(title=f"Item {i}"))
        return storage

    def test_create_no_key(self):
        storage = self.create_composite_storage()
        item = storage.create(dict(title="No Key"))
        self.assertEqual(dict(id="a/101", title="No Key"), item)
        item = storage.read("a/101")
        self.assertEqual(dict(id="a/101", title="No Key"), item)
        self.assertEqual(101, storage.count())

    def test_create_with_key(self):
        storage = self.create_composite_storage()
        item = storage.create(dict(id="b/101", title="B Key"))
        self.assertEqual(dict(id="b/101", title="B Key"), item)
        item = storage.read("b/101")
        self.assertEqual(dict(id="b/101", title="B Key"), item)
        self.assertEqual(101, storage.count())

    def test_create_with_invalid_key(self):
        storage = self.create_composite_storage()
        error = None
        try:
            storage.create(dict(id="c/101", title="C Key"))
        except PersistyError as e:
            error = e
            self.assertEqual(100, storage.count())
        self.assertIsNotNone(error)

    def test_read_with_missing_key(self):
        storage = self.create_composite_storage()
        self.assertIsNone(storage.read("b/101"))

    def test_read_with_invalid_key(self):
        storage = self.create_composite_storage()
        error = None
        try:
            storage.read("c/55")
        except PersistyError as e:
            error = e
        self.assertIsNotNone(error)

    def test_update(self):
        storage = self.create_composite_storage()
        updates = dict(id="b/3", title="Updated Item")
        item = storage.update(updates)
        self.assertIsNot(updates, item)
        self.assertEqual(updates, item)
        read = storage.read("b/3")
        self.assertEqual(read, item)
        self.assertEqual(100, storage.count())

    def test_update_with_missing_key(self):
        storage = self.create_composite_storage()
        self.assertIsNone(storage.update(dict(id="b/101", title="Updated Item")))
        self.assertEqual(100, storage.count())

    def test_update_with_invalid_key(self):
        storage = self.create_composite_storage()
        error = None
        try:
            storage.update(dict(id="c/55", title="Updated Item"))
        except PersistyError as e:
            error = e
        self.assertIsNotNone(error)
        self.assertEqual(100, storage.count())

    def test_delete(self):
        storage = self.create_composite_storage()
        deleted = storage.delete("b/3")
        self.assertTrue(deleted)
        self.assertEqual(99, storage.count())
        deleted = storage.delete("b/3")
        self.assertFalse(deleted)
        self.assertEqual(99, storage.count())

    def test_delete_with_invalid_key(self):
        storage = self.create_composite_storage()
        error = None
        try:
            storage.delete("c/55")
        except PersistyError as e:
            error = e
        self.assertIsNotNone(error)

    def test_count_with_search_filter(self):
        storage = self.create_composite_storage()
        search_filter = filter_factory(CompositeItem).title.gt("Item 50")
        count = storage.count(search_filter)
        self.assertEqual(53, count)

    def test_count_with_search_filter_on_key(self):
        storage = self.create_composite_storage()
        search_filter = filter_factory(CompositeItem).id.gt("b/53")
        count = storage.count(search_filter)
        self.assertEqual(25, count)

    def test_search_all_with_search_filter(self):
        storage = self.create_composite_storage()
        search_filter = filter_factory(CompositeItem).title.gt("Item 94")
        results = {i["id"]: i["title"] for i in storage.search_all(search_filter)}
        expected = {
            ("a", "b")[i % 2] + "/" + str(i): f"Item {i}" for i in range(95, 100)
        }
        self.assertEqual(expected, results)

    def test_search_with_search_filter_on_key(self):
        storage = self.create_composite_storage()
        search_filter = filter_factory(CompositeItem).id.gt("b/90")
        results = {i["id"]: i["title"] for i in storage.search_all(search_filter)}
        expected = {f"b/{i}": f"Item {i}" for i in range(91, 101, 2)}
        self.assertEqual(expected, results)

    def test_search_with_search_filter_on_key_and_sort_order(self):
        storage = self.create_composite_storage()
        filters = filter_factory(CompositeItem)
        search_filter = filters.title.gt("Item 94")
        results = list(storage.search_all(search_filter, filters.title.desc()))
        expected = [
            dict(id=("a", "b")[i % 2] + "/" + str(i), title=f"Item {i}")
            for i in range(99, 94, -1)
        ]
        self.assertEqual(expected, results)

    def test_count_composite_filter(self):
        storage = self.create_composite_storage()
        filters = filter_factory(CompositeItem)
        search_filter = Not(filters.title.gt("Item 94") & filters.title.lt("Item 98"))
        count = storage.count(search_filter)
        self.assertEqual(97, count)


@stored
class CompositeItem:
    id: str
    title: str
