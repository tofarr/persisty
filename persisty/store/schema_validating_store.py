from dataclasses import dataclass
from typing import Optional

from marshy import get_default_context
from marshy.marshaller.marshaller_abc import MarshallerABC
from schemey import Schema, get_default_schema_context

from persisty.errors import PersistyError
from persisty.store.filtered_store_abc import FilteredStoreABC, T
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta


@dataclass(frozen=True)
class SchemaValidatingStore(FilteredStoreABC[T]):
    store: StoreABC[T]
    schema_for_create: Schema = None
    schema_for_update: Schema = None
    marshaller_for_create: MarshallerABC[T] = None
    marshaller_for_update: MarshallerABC[T] = None

    def __post_init__(self):
        if not self.schema_for_create:
            object.__setattr__(
                self,
                "schema_for_create",
                get_default_schema_context().schema_from_type(
                    self.get_meta().get_create_dataclass()
                ),
            )
        if not self.schema_for_update:
            object.__setattr__(
                self,
                "schema_for_update",
                get_default_schema_context().schema_from_type(
                    self.get_meta().get_update_dataclass()
                ),
            )
        if not self.marshaller_for_create:
            object.__setattr__(
                self,
                "marshaller_for_create",
                get_default_context().get_marshaller(
                    self.get_meta().get_create_dataclass()
                ),
            )
        if not self.marshaller_for_update:
            object.__setattr__(
                self,
                "marshaller_for_update",
                get_default_context().get_marshaller(
                    self.get_meta().get_update_dataclass()
                ),
            )

    def get_store(self) -> StoreABC:
        return self.store

    def get_meta(self) -> StoreMeta:
        return self.store.get_meta()

    def filter_create(self, item: T) -> Optional[T]:
        dumped = self.marshaller_for_create.dump(item)
        error = next(self.schema_for_create.iter_errors(dumped), None)
        if error:
            raise PersistyError(error)
        return item

    def filter_update(self, old_item: T, updates: T) -> T:
        new_item = {
            **self.marshaller_for_update.dump(old_item),
            **self.marshaller_for_update.dump(updates),
        }
        error = next(self.schema_for_update.iter_errors(new_item), None)
        if error:
            raise PersistyError(error)
        return updates
