from dataclasses import dataclass

from marshy.types import ExternalItemType
from schemey.object_schema import ObjectSchema
from schemey.schema_abc import SchemaABC

from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta
from persisty.storage.wrapper_storage_abc import WrapperStorageABC


@dataclass(frozen=True)
class SchemaValidatingStorage(WrapperStorageABC):
    storage: StorageABC
    schema: SchemaABC = None

    def __post_init__(self):
        if self.schema:
            return
        storage_meta = self.get_storage().get_storage_meta()
        return ObjectSchema(
            name=storage_meta.name,
            properties={f.name: f.schema for f in storage_meta.fields}
        )

    def get_storage(self) -> StorageABC:
        return self.storage

    def get_storage_meta(self) -> StorageMeta:
        return self.get_storage().get_storage_meta()

    def filter_create(self, item: ExternalItemType) -> ExternalItemType:
        self.schema.validate(item)
        return item

    def filter_update(self, old_item: ExternalItemType, updates: ExternalItemType) -> ExternalItemType:
        new_item = {**old_item, **updates}
        if not next(self.schema.get_schema_errors(new_item), None):
            return updates
