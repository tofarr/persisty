from dataclasses import dataclass

from marshy.types import ExternalItemType
from schemey.object_schema import ObjectSchema
from schemey.schema_abc import SchemaABC
from schemey.schema_error import SchemaError

from persisty.errors import PersistyError
from persisty.storage.filtered_storage_abc import FilteredStorageABC
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta


@dataclass(frozen=True)
class SchemaValidatingStorage(FilteredStorageABC):
    storage: StorageABC
    schema: SchemaABC = None

    def __post_init__(self):
        if self.schema:
            return
        storage_meta = self.get_storage().get_storage_meta()
        schema = ObjectSchema(
            name=storage_meta.name,
            properties={f.name: f.schema for f in storage_meta.fields},
        )
        object.__setattr__(self, 'schema', schema)

    def get_storage(self) -> StorageABC:
        return self.storage

    def get_storage_meta(self) -> StorageMeta:
        return self.storage.get_storage_meta()

    def filter_create(self, item: ExternalItemType) -> ExternalItemType:
        try:
            self.schema.validate(item)
        except SchemaError as e:
            raise PersistyError(e)
        return item

    def filter_update(
        self, old_item: ExternalItemType, updates: ExternalItemType
    ) -> ExternalItemType:
        new_item = {**old_item, **updates}
        if not next(self.schema.get_schema_errors(new_item), None):
            return updates
