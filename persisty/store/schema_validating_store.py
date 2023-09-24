from dataclasses import dataclass
from typing import Optional

from marshy import get_default_context
from marshy.marshaller.marshaller_abc import MarshallerABC
from schemey import Schema, get_default_schema_context

from persisty.errors import PersistyError
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store.filtered_store_abc import FilteredStoreABC, T
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta
from persisty.util import UNDEFINED


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

    def filter_update(self, item: T, updates: T) -> T:
        new_item = {
            **self.marshaller_for_update.dump(item),
            **self.marshaller_for_update.dump(updates),
        }
        error = next(self.schema_for_update.iter_errors(new_item), None)
        if error:
            raise PersistyError(error)
        return updates

    def update_all(self, search_filter: SearchFilterABC[T], updates: T):
        # Validate, but raise errors only for defined values
        updates_dict = self.marshaller_for_update.dump(updates)
        errors = self.schema_for_update.iter_errors(updates_dict)
        for error in errors:
            attr_name = error.json_path.split('.')[1]
            attr_value = getattr(updates, attr_name, UNDEFINED)
            if attr_value is not UNDEFINED:
                raise PersistyError(error)
        super().update_all(search_filter, updates)
