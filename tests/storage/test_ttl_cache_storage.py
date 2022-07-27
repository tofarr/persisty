from datetime import datetime
from unittest import TestCase
from uuid import uuid4

from persisty.impl.mem.mem_storage import mem_storage
from persisty.obj_storage.stored import get_storage_meta
from persisty.storage.batch_edit import Create, Update, Delete
from persisty.field.field_filter import FieldFilter, FieldFilterOp
from persisty.storage.ttl_cache_storage import TtlCacheStorage
from tests.fixtures.number_name import NumberName


class TestTtlCacheStorage(TestCase):
    @staticmethod
    def new_number_name_storage() -> TtlCacheStorage:
        storage = mem_storage(get_storage_meta(NumberName))
        storage = TtlCacheStorage(storage)
        return storage

    def test_create(self):
        storage = self.new_number_name_storage()
        now = datetime.now().isoformat()
        item = storage.create(dict(title="Ten", value=10))
        self.assertIsNotNone(item["id"])
        self.assertTrue(item["created_at"] >= now)
        self.assertTrue(
            abs(datetime.now().timestamp() - datetime.now().timestamp()) < 0.01
        )

    def test_read(self):
        storage = self.new_number_name_storage()
        item = storage.create(dict(title="Ten", value=10))
        updated_item = storage.storage.update(dict(id=item["id"], title="Zehn"))
        self.assertEqual("Zehn", updated_item["title"])
        read = storage.read(item["id"])
        self.assertEqual("Ten", read["title"])
        storage.clear_cache()
        read = storage.read(item["id"])
        self.assertEqual("Zehn", read["title"])

    def test_update(self):
        storage = self.new_number_name_storage()
        item = storage.create(dict(title="Ten", value=10))
        updated_item = storage.update(dict(id=item["id"], title="Zehn"))
        self.assertEqual("Zehn", updated_item["title"])
        read = storage.read(item["id"])
        self.assertEqual("Zehn", read["title"])

    def test_delete(self):
        storage = self.new_number_name_storage()
        item = storage.create(dict(title="Ten", value=10))
        self.assertTrue(storage.delete(item["id"]))
        read = storage.read(item["id"])
        self.assertIsNone(read)

    def test_read_missing(self):
        storage = self.new_number_name_storage()
        self.assertIsNone(storage.read(str(uuid4())))

    # noinspection PyUnresolvedReferences
    def test_read_batch(self):
        storage = self.new_number_name_storage()
        results = storage.edit_batch([Create(dict(title=str(i))) for i in range(1, 10)])
        items = storage.read_batch(
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
        updated_item = storage.storage.update(
            dict(id=items[1]["id"], title="8 Updated")
        )
        cached_items = storage.read_batch(
            [
                results[7].edit.item["id"],
                results[0].edit.item["id"],
            ]
        )
        self.assertNotEqual(cached_items[0], items[1])
        self.assertEqual(cached_items[1], items[0])
        storage.clear_cache()
        cached_items = storage.read_batch(
            [
                results[7].edit.item["id"],
                results[0].edit.item["id"],
            ]
        )
        self.assertEqual(cached_items[0], updated_item)
        self.assertEqual(cached_items[1], items[0])

    # noinspection PyUnresolvedReferences
    def test_search(self):
        storage = self.new_number_name_storage()
        storage.edit_batch([Create(dict(title=str(i))) for i in range(1, 10)])
        timestamp = datetime.now().isoformat()
        results = list(storage.search_all())
        self.assertEqual([str(i) for i in range(1, 10)], [i["title"] for i in results])
        # Cached values don't include update that went aroudn the cache
        storage.storage.update(dict(id=results[1]["id"], title="Updated"))
        self.assertEqual(results, list(storage.search_all()))
        # Clearing the cache fixes this
        storage.clear_cache()
        new_results = list(storage.search_all())
        self.assertEqual("Updated", new_results[1]["title"])
        new_results.pop(1)
        new_results_2 = list(
            storage.search_all(FieldFilter("updated_at", FieldFilterOp.lt, timestamp))
        )
        self.assertEqual(new_results, new_results_2)

    # noinspection PyUnresolvedReferences
    def test_edit_batch(self):
        storage = self.new_number_name_storage()
        results = storage.edit_batch([Create(dict(title=str(i))) for i in range(1, 10)])
        self.assertEqual(9, storage.count())
        self.assertEqual(5, storage.count(FieldFilter("title", FieldFilterOp.gte, "5")))
        storage.edit_batch(
            [
                Update(dict(id=results[0].edit.item["id"], title=10)),
                Update(dict(id=results[1].edit.item["id"], title="Two")),
                Delete(results[2].edit.item["id"]),
            ]
        )
        self.assertIsNone(storage.read(results[2].edit.item["id"]))
