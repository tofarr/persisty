from decimal import Decimal
from typing import List
from unittest import TestCase

from marshy import dump
from marshy.types import ExternalItemType

from persisty.attr.attr import Attr
from persisty.attr.attr_type import AttrType
from persisty.attr.generator.default_value_generator import DefaultValueGenerator
from persisty.impl.dynamodb.dynamodb_index import DynamodbIndex
from persisty.impl.dynamodb.dynamodb_store_factory import DynamodbStoreFactory
from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.key_config.composite_key_config import CompositeKeyConfig
from persisty.result_set import ResultSet

from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.filter_factory import filter_factory

from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC
from persisty.store_meta import get_meta
from persisty.stored import stored
from tests.fixtures.number_name import NumberName, NUMBER_NAMES_DICTS
from tests.fixtures.storage_tst_abc import StoreTstABC
from tests.fixtures.super_bowl_results import SuperBowlResult, SUPER_BOWL_RESULT_DICTS
from tests.utils import mock_dynamodb_with_super


@mock_dynamodb_with_super
class TestDynamodbStore(TestCase, StoreTstABC):
    def new_super_bowl_results_store(self) -> StoreABC:
        store_factory = DynamodbStoreFactory(meta=get_meta(SuperBowlResult))
        self.seed_table(store_factory, SUPER_BOWL_RESULT_DICTS)
        store = store_factory.create()
        return store

    def new_number_name_store(self) -> StoreABC:
        store_factory = DynamodbStoreFactory(meta=get_meta(NumberName))
        self.seed_table(store_factory, NUMBER_NAMES_DICTS)
        store = store_factory.create()
        return store

    @staticmethod
    def seed_table(store_factory: DynamodbStoreFactory, items: List[ExternalItemType]):
        store_factory.derive_from_meta()
        store_factory.create_table_in_aws()
        dynamodb = store_factory.get_session().resource("dynamodb")
        table = dynamodb.Table(store_factory.table_name)
        with table.batch_writer() as batch:
            for result in items:
                batch.put_item(result)

    def test_update_with_sk(self):
        tag_store = self.new_tag_store()
        tag = Tag(pk=3, sk=5, title="Updated")
        key = tag_store.get_meta().key_config.to_key_str(tag)
        item = tag_store.update(tag)
        tag.codes = ["5"]
        self.assertEqual(round(1 / item.weight), 305)
        tag.weight = item.weight
        self.assertEqual(tag, item)
        loaded = tag_store.read(key)
        self.assertEqual(loaded, tag)

    def test_dynamodb_update_fail_filter(self):
        # noinspection PyUnresolvedReferences
        store = self.new_number_name_store().store
        self.spec_for_update_fail_filter(store)

    def test_read_with_sk_missing(self):
        tag_store = self.new_tag_store()
        key = Tag(pk=3, sk=5000)
        key = tag_store.get_meta().key_config.to_key_str(key)
        item = tag_store.read(key)
        self.assertIsNone(item)

    def test_search_exclude_all(self):
        # noinspection PyTypeChecker
        tag_store: WrapperStoreABC = self.new_tag_store()
        result_set = tag_store.get_store().search(EXCLUDE_ALL)
        self.assertEqual(ResultSet([]), result_set)

    def test_create_with_float(self):
        tag_store = self.new_tag_store()
        tag = Tag(
            pk=20,
            sk=20,
            title="Two thousand and twenty",
            codes=["foo", "bar"],
            weight=0.2,
        )
        tag_store.create(tag)
        key = tag_store.get_meta().key_config.to_key_str(tag)
        results = list(tag_store.search_all())
        self.assertEqual(1001, len(results))
        item = tag_store.read(key)
        self.assertEqual(item, tag)

    def test_create_with_float_int(self):
        tag_store = self.new_tag_store()
        tag = Tag(
            pk=20,
            sk=20,
            title="Two thousand and twenty",
            codes=["foo", "bar"],
            weight=1,
        )
        tag_store.create(tag)
        key = tag_store.get_meta().key_config.to_key_str(Tag(pk=20, sk=20))
        item = tag_store.read(key)
        self.assertEqual(item, tag)

    def test_count_multi_page(self):
        tag_store = self.new_tag_store()
        filters = filter_factory(Tag)
        self.assertEqual(1000, tag_store.count(filters.title.ne("foobar")))
        self.assertEqual(10, tag_store.count(filters.sk.eq(10)))

    def test_convert_to_decimals(self):
        item = {"some_int": 10, "some_float": 0.5}
        # noinspection PyUnresolvedReferences
        store = self.new_tag_store()
        # noinspection PyUnresolvedReferences
        store = store.store
        converted = store._convert_to_decimals(item)
        expected = {"some_int": Decimal("10"), "some_float": Decimal(0.5)}
        self.assertEqual(expected, converted)

    def test_dammit(self):
        self.test_create_with_float()

    def new_tag_store(self) -> StoreABC:
        store_meta = get_meta(Tag)
        store_factory = DynamodbStoreFactory(
            meta=store_meta,
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
        self.seed_table(store_factory, tags)
        store = store_factory.create()
        return store


@stored(
    batch_size=10,
    key_config=CompositeKeyConfig(
        (AttrKeyConfig("pk", AttrType.INT), AttrKeyConfig("sk", AttrType.INT))
    ),
)
class Tag:
    pk: int
    sk: int
    title: str
    codes: List[str] = Attr(create_generator=DefaultValueGenerator([]))
    weight: float = 0.5
