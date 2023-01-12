from datetime import datetime
from unittest import TestCase
from uuid import uuid4

from persisty.impl.mem.mem_store import mem_store
from persisty.obj_store.stored import get_meta
from persisty.store.batch_edit import Create, Update, Delete
from persisty.attr.attr_filter import FieldFilter, FieldFilterOp
from persisty.store.ttl_cache_store import TtlCacheStore
from tests.fixtures.number_name import NumberName


class TestTtlCacheStore(TestCase):
    @staticmethod
    def new_number_name_store() -> TtlCacheStore:
        store = mem_store(get_meta(NumberName))
        store = TtlCacheStore(store)
        return store

    def test_create(self):
        store = self.new_number_name_store()
        now = datetime.now().isoformat()
        item = store.create(dict(title="Ten", value=10))
        self.assertIsNotNone(item["id"])
        self.assertTrue(item["created_at"] >= now)
        self.assertTrue(
            abs(datetime.now().timestamp() - datetime.now().timestamp()) < 0.01
        )

    def test_read(self):
        store = self.new_number_name_store()
        item = store.create(dict(title="Ten", value=10))
        updated_item = store.store.update(dict(id=item["id"], title="Zehn"))
        self.assertEqual("Zehn", updated_item["title"])
        read = store.read(item["id"])
        self.assertEqual("Ten", read["title"])
        store.clear_cache()
        read = store.read(item["id"])
        self.assertEqual("Zehn", read["title"])

    def test_update(self):
        store = self.new_number_name_store()
        item = store.create(dict(title="Ten", value=10))
        updated_item = store.update(dict(id=item["id"], title="Zehn"))
        self.assertEqual("Zehn", updated_item["title"])
        read = store.read(item["id"])
        self.assertEqual("Zehn", read["title"])

    def test_delete(self):
        store = self.new_number_name_store()
        item = store.create(dict(title="Ten", value=10))
        self.assertTrue(store.delete(item["id"]))
        read = store.read(item["id"])
        self.assertIsNone(read)

    def test_read_missing(self):
        store = self.new_number_name_store()
        self.assertIsNone(store.read(str(uuid4())))

    # noinspection PyUnresolvedReferences
    def test_read_batch(self):
        store = self.new_number_name_store()
        results = store.edit_batch([Create(dict(title=str(i))) for i in range(1, 10)])
        items = store.read_batch(
            [
                results[0].edit.item["id"],
                results[7].edit.item["id"],
                results[2].edit.item["id"],
                str(uuid4()),
            ]
        )
        self.assertEqual("1", items[0]["title"])
        self.assertEqual("8", items[1]["title"])
        self.assertEqual("3", items[2]["title"])
        self.assertIsNone(items[3])
        updated_item = store.store.update(
            dict(id=items[1]["id"], title="8 Updated")
        )
        cached_items = store.read_batch(
            [
                results[7].edit.item["id"],
                results[0].edit.item["id"],
            ]
        )
        self.assertNotEqual(cached_items[0], items[1])
        self.assertEqual(cached_items[1], items[0])
        store.clear_cache()
        cached_items = store.read_batch(
            [
                results[7].edit.item["id"],
                results[0].edit.item["id"],
            ]
        )
        self.assertEqual(cached_items[0], updated_item)
        self.assertEqual(cached_items[1], items[0])

    # noinspection PyUnresolvedReferences
    def test_search(self):
        store = self.new_number_name_store()
        store.edit_batch([Create(dict(title=str(i))) for i in range(1, 10)])
        timestamp = datetime.now().isoformat()
        results = list(store.search_all())
        self.assertEqual([str(i) for i in range(1, 10)], [i["title"] for i in results])
        # Cached values don't include update that went aroudn the cache
        store.store.update(dict(id=results[1]["id"], title="Updated"))
        self.assertEqual(results, list(store.search_all()))
        # Clearing the cache fixes this
        store.clear_cache()
        new_results = list(store.search_all())
        self.assertEqual("Updated", new_results[1]["title"])
        new_results.pop(1)
        new_results_2 = list(
            store.search_all(FieldFilter("updated_at", FieldFilterOp.lt, timestamp))
        )
        self.assertEqual(new_results, new_results_2)

    # noinspection PyUnresolvedReferences
    def test_edit_batch(self):
        store = self.new_number_name_store()
        results = store.edit_batch([Create(dict(title=str(i))) for i in range(1, 10)])
        self.assertEqual(9, store.count())
        self.assertEqual(5, store.count(FieldFilter("title", FieldFilterOp.gte, "5")))
        store.edit_batch(
            [
                Update(dict(id=results[0].edit.item["id"], title=10)),
                Update(dict(id=results[1].edit.item["id"], title="Two")),
                Delete(results[2].edit.item["id"]),
            ]
        )
        self.assertIsNone(store.read(results[2].edit.item["id"]))
