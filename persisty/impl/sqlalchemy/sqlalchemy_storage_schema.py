from dataclasses import dataclass, field
from typing import Iterator, Optional

from schemey import SchemaContext, get_default_schema_context
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine

from persisty.access_control.authorization import Authorization
from persisty.context.meta_storage_abc import STORED_STORAGE_META
from persisty.context.storage_schema_abc import StorageSchemaABC
from persisty.impl.sqlalchemy.sqlalchemy_connector import get_default_engine
from persisty.impl.sqlalchemy.sqlalchemy_table_converter import SqlalchemyTableConverter
from persisty.impl.sqlalchemy.sqlalchemy_table_storage import SqlalchemyTableStorage
from persisty.storage.schema_validating_storage import SchemaValidatingStorage
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta


@dataclass
class SqlalchemyStorageSchema(StorageSchemaABC):
    name: str = "sql"
    engine: Engine = field(default_factory=get_default_engine)
    converter: Optional[SqlalchemyTableConverter] = None
    metadata_storage: Optional[StorageABC] = None
    schema_context: SchemaContext = field(default_factory=get_default_schema_context)

    def __post_init__(self):
        if self.converter is None:
            self.converter = SqlalchemyTableConverter(self.engine, MetaData())
        if self.metadata_storage is None:
            metadata_table = self.converter.to_sqlalchemy_table(STORED_STORAGE_META)
            metadata_table.create(bind=Engine, checkfirst=True)
            metadata_storage = SchemaValidatingStorage(
                SqlalchemyTableStorage(STORED_STORAGE_META, metadata_table, self.engine)
            )
            self.metadata_storage = metadata_storage

    def get_name(self) -> str:
        return self.name

    def create_storage(
        self, storage_meta: StorageMeta, authorization: Authorization
    ) -> Optional[StorageABC]:
        if storage_meta.name == STORED_STORAGE_META.name:
            return
        table = self.converter.to_sqlalchemy_table(storage_meta)
        table.create(bind=self.engine, checkfirst=True)
        storage = SqlalchemyTableStorage(storage_meta, table, self.engine)
        storage = SchemaValidatingStorage(storage, storage_meta.to_schema())
        return storage

    def get_storage_by_name(
        self, storage_name: str, authorization: Authorization
    ) -> Optional[StorageABC]:
        if storage_name == STORED_STORAGE_META.name:
            return
        storage_meta = self.metadata_storage.read(storage_name)
        if storage_meta:
            storage_meta = self.schema_context.marshaller_context.load(
                StorageMeta, storage_meta
            )
            table = self.converter.to_sqlalchemy_table(storage_meta)
            storage = SqlalchemyTableStorage(storage_meta, table, self.engine)
            storage = SchemaValidatingStorage(storage, storage_meta.to_schema())
            return storage

    def get_all_storage_meta(self) -> Iterator[StorageMeta]:
        marshaller = self.schema_context.marshaller_context.get_marshaller(StorageMeta)
        for storage_meta in self.metadata_storage.search_all():
            storage_meta = marshaller.load(storage_meta)
            yield storage_meta
