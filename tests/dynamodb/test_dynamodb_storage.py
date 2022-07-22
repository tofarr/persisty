from typing import List
from unittest import TestCase

from marshy.types import ExternalItemType

from persisty.impl.dynamodb.dynamodb_storage_factory import DynamodbStorageFactory
from persisty.obj_storage.stored import get_storage_meta
from persisty.storage.storage_abc import StorageABC
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

    def seed_table(self, storage_factory: DynamodbStorageFactory, items: List[ExternalItemType]):
        storage_factory.sanitize_storage_meta()
        storage_factory.derive_from_storage_meta()
        storage_factory.create_table_in_aws()
        dynamodb = storage_factory.get_session().resource('dynamodb')
        table = dynamodb.Table(storage_factory.table_name)
        with table.batch_writer() as batch:
            for result in items:
                batch.put_item(result)

    def test_edit_all2(self):
        self.test_edit_all()
