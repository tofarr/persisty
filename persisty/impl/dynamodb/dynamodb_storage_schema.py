from dataclasses import dataclass, field
from typing import Iterator, Optional

from schemey import SchemaContext, get_default_schema_context

from persisty.access_control.authorization import Authorization
from persisty.context.meta_storage_abc import STORED_STORAGE_META
from persisty.context.storage_schema_abc import StorageSchemaABC
from persisty.impl.dynamodb.dynamodb_storage_factory import DynamodbStorageFactory
from persisty.storage.schema_validating_storage import SchemaValidatingStorage
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta


@dataclass
class DynamodbStorageSchema(StorageSchemaABC):
    name: str = 'dynamodb'
    aws_profile_name: Optional[str] = None
    region_name: Optional[str] = None
    metadata_storage: Optional[StorageABC] = None
    schema_context: SchemaContext = field(default_factory=get_default_schema_context)

    def __post_init__(self):
        if self.metadata_storage is None:
            if self.metadata_storage:
                return
            factory = DynamodbStorageFactory(
                storage_meta=STORED_STORAGE_META,
                aws_profile_name=self.aws_profile_name,
                region_name=self.region_name
            )
            factory.derive_from_storage_meta()
            try:
                factory.load_from_aws()
            except ValueError:
                factory.create_table_in_aws()
            self.metadata_storage = SchemaValidatingStorage(factory.create_storage())

    def get_name(self) -> str:
        return self.name

    def create_storage(self, storage_meta: StorageMeta, authorization: Authorization) -> Optional[StorageABC]:
        factory = DynamodbStorageFactory(
            storage_meta=storage_meta,
            aws_profile_name=self.aws_profile_name,
            region_name=self.region_name
        )
        factory.derive_from_storage_meta()
        factory.create_table_in_aws()
        return SchemaValidatingStorage(factory.create_storage())

    def get_storage_by_name(self, storage_name: str, authorization: Authorization) -> Optional[StorageABC]:
        storage_meta = self.metadata_storage.read(storage_name)
        if storage_meta is None:
            return
        storage_meta = self.schema_context.marshaller_context.load(StorageMeta, storage_meta)
        factory = DynamodbStorageFactory(
            storage_meta=storage_meta,
            aws_profile_name=self.aws_profile_name,
            region_name=self.region_name
        )
        factory.load_from_aws() # Grab indexes and such not in AWS
        return factory.create_storage()

    def get_all_storage_meta(self) -> Iterator[StorageMeta]:
        marshaller = self.schema_context.marshaller_context.get_marshaller(StorageMeta)
        for storage_meta in self.metadata_storage.search_all():
            storage_meta = marshaller.load(storage_meta)
            yield storage_meta
