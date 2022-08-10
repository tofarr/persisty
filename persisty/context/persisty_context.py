from dataclasses import dataclass, field
from typing import List, Optional, Iterator

from schemey import SchemaContext, get_default_schema_context

from persisty.access_control.authorization import Authorization
from persisty.context.storage_schema_abc import StorageSchemaABC
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta


@dataclass(frozen=True)
class PersistyContext:
    """Links dynamic access control with meta storage"""

    storage_schemas: List[StorageSchemaABC] = field(default_factory=list)
    schema_context: SchemaContext = field(default_factory=get_default_schema_context)

    def register_storage_schema(self, storage_schema: StorageSchemaABC):
        self.storage_schemas.append(storage_schema)
        self.storage_schemas.sort(key=lambda f: f.priority, reverse=True)

    def create_storage(
        self, storage_meta: StorageMeta, authorization: Authorization
    ) -> Optional[StorageABC]:
        for storage_schema in self.storage_schemas:
            storage = storage_schema.create_storage(storage_meta, authorization)
            if storage:
                return storage

    def get_storage_by_name(
        self, storage_name: str, authorization: Authorization
    ) -> Optional[StorageABC]:
        for storage_schema in self.storage_schemas:
            storage = storage_schema.get_storage_by_name(storage_name, authorization)
            if storage:
                return storage

    def get_all_storage_meta(self) -> Iterator[StorageMeta]:
        names = set()
        for storage_schema in self.storage_schemas:
            for storage_meta in storage_schema.get_all_storage_meta():
                if storage_meta.name not in names:
                    names.add(storage_meta.name)
                    yield storage_meta

    def get_schema_by_name(self, schema_name: str) -> Optional[StorageSchemaABC]:
        for storage_schema in self.storage_schemas:
            if storage_schema.get_name() == schema_name:
                return storage_schema
