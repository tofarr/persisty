from dataclasses import dataclass
from typing import Optional

from marshy.types import ExternalItemType
from schemey import Schema

from persisty.errors import PersistyError
from persisty.storage.filtered_storage_abc import FilteredStorageABC
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta


@dataclass(frozen=True)
class SchemaValidatingStorage(FilteredStorageABC):
    storage: StorageABC
    schema: Schema = None

    def __post_init__(self):
        if not self.schema:
            storage_meta = self.get_storage().get_storage_meta()
            schema = storage_meta.to_schema()
            object.__setattr__(self, "schema", schema)

    def get_storage(self) -> StorageABC:
        return self.storage

    def get_storage_meta(self) -> StorageMeta:
        return self.storage.get_storage_meta()

    def filter_create(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        error = next(self.schema.iter_errors(item), None)
        if error:
            raise PersistyError(error)
        return item

    def filter_update(
        self, old_item: ExternalItemType, updates: ExternalItemType
    ) -> ExternalItemType:
        new_item = {**old_item, **updates}
        error = next(self.schema.iter_errors(new_item), None)
        if error:
            raise PersistyError(error)
        return updates
