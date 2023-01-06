from decimal import Decimal
from typing import List
from unittest import TestCase

from marshy import dump
from marshy.types import ExternalItemType

from persisty.impl.dynamodb.dynamodb_index import DynamodbIndex
from persisty.impl.dynamodb.dynamodb_storage_factory import DynamodbStorageFactory
from persisty.obj_storage.attr import Attr
from persisty.obj_storage.filter_factory import filter_factory
from persisty.obj_storage.stored import get_storage_meta, stored
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.field.write_transform.default_value_transform import (
    DefaultValueTransform,
)
from persisty.storage.result_set import ResultSet
from persisty.storage.storage_abc import StorageABC
from persisty.storage.wrapper_storage_abc import WrapperStorageABC
from tests.fixtures.number_name import NumberName, NUMBER_NAMES_DICTS
from tests.fixtures.super_bowl_results import SuperBowlResult, SUPER_BOWL_RESULT_DICTS
from tests.fixtures.storage_tst_abc import StorageTstABC
from tests.utils import mock_dynamodb_with_super


@mock_dynamodb_with_super
class TestDynamodbStorage(TestCase, StorageTstABC):
    def new_super_bowl_results_storage(self) -> StorageABC:
        storage_factory = DynamodbStorageFactory(
            storage_meta=get_storage_meta(SuperBowlResult)
        )
        self.seed_table(storage_factory, SUPER_BOWL_RESULT_DICTS)
        storage = storage_factory.create_storage()
        return storage

    def new_number_name_storage(self) -> StorageABC:
        storage_factory = DynamodbStorageFactory(
            storage_meta=get_storage_meta(NumberName)
        )
        self.seed_table(storage_factory, NUMBER_NAMES_DICTS)
        storage = storage_factory.create_storage()
        return storage

    @staticmethod
    def seed_table(
        storage_factory: DynamodbStorageFactory, items: List[ExternalItemType]
    ):
        storage_factory.sanitize_storage_meta()
        storage_factory.derive_from_storage_meta()
        storage_factory.create_table_in_aws()
        dynamodb = storage_factory.get_session().resource("dynamodb")
        table = dynamodb.Table(storage_factory.table_name)
        with table.batch_writer() as batch:
            for result in items:
                batch.put_item(result)

    def test_update_with_sk(self):
        tag_storage = self.new_tag_storage()
        tag = dict(pk=3, sk=5, title="Updated")
        key = tag_storage.get_storage_meta().key_config.to_key_str(tag)
        item = tag_storage.update(tag)
        tag["codes"] = ["5"]
        self.assertEqual(round(1 / item["weight"]), 305)
        tag["weight"] = item["weight"]
        self.assertEqual(tag, item)
        loaded = tag_storage.read(key)
        self.assertEqual(loaded, tag)

    def test_dynamodb_update_fail_filter(self):
        # noinspection PyUnresolvedReferences
        storage = self.new_number_name_storage().storage.storage
        self.spec_for_update_fail_filter(storage)

    def test_read_with_sk_missing(self):
        tag_storage = self.new_tag_storage()
        key = dict(pk=3, sk=5000)
        key = tag_storage.get_storage_meta().key_config.to_key_str(key)
        item = tag_storage.read(key)
        self.assertIsNone(item)

    def test_search_exclude_all(self):
        # noinspection PyTypeChecker
        tag_storage: WrapperStorageABC = self.new_tag_storage()
        result_set = tag_storage.get_storage().search(EXCLUDE_ALL)
        self.assertEqual(ResultSet([]), result_set)

    def test_create_with_float(self):
        tag_storage = self.new_tag_storage()
        kwargs = dict(
            pk=20,
            sk=20,
            title="Two thousand and twenty",
            codes=["foo", "bar"],
            weight=0.2,
        )
        tag_storage.create(dump(Tag(**kwargs)))
        key = tag_storage.get_storage_meta().key_config.to_key_str(dict(pk=20, sk=20))
        item = tag_storage.read(key)
        self.assertEqual(item, kwargs)

    def test_create_with_float_int(self):
        tag_storage = self.new_tag_storage()
        kwargs = dict(
            pk=20,
            sk=20,
            title="Two thousand and twenty",
            codes=["foo", "bar"],
            weight=1.0,
        )
        tag_storage.create(dump(Tag(**kwargs)))
        key = tag_storage.get_storage_meta().key_config.to_key_str(dict(pk=20, sk=20))
        item = tag_storage.read(key)
        self.assertEqual(item, kwargs)

    def test_count_multi_page(self):
        tag_storage = self.new_tag_storage()
        filters = filter_factory(Tag)
        self.assertEqual(1000, tag_storage.count(filters.title.ne("foobar")))
        self.assertEqual(10, tag_storage.count(filters.sk.eq(10)))

    def test_convert_to_decimals(self):
        item = {"some_int": 10, "some_float": 0.5}
        # noinspection PyUnresolvedReferences
        storage = self.new_tag_storage()
        storage = storage.storage
        converted = storage._convert_to_decimals(item)
        expected = {"some_int": Decimal("10"), "some_float": Decimal(0.5)}
        self.assertEqual(expected, converted)

    def new_tag_storage(self) -> StorageABC:
        storage_meta = get_storage_meta(Tag)
        storage_factory = DynamodbStorageFactory(
            storage_meta=storage_meta,
            index=DynamodbIndex("pk", "sk"),
            global_secondary_indexes=dict(gix__sk__pk=DynamodbIndex("sk", "pk")),
        )
        tags = list(
            dump(
                Tag(
                    int(i / 100),
                    i % 100,
                    str(i),
                    [str(c) for c in (2, 3, 5) if not i % c],
                    Decimal("%.9f" % (1 / i)),
                )
            )
            for i in range(1, 1001)
        )
        self.seed_table(storage_factory, tags)
        storage = storage_factory.create_storage()
        return storage


@stored(batch_size=10)
class Tag:
    pk: int
    sk: int
    title: str
    codes: List[str] = Attr(write_transform=DefaultValueTransform([]))
    weight: float = 0.5
