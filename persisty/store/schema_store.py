from dataclasses import dataclass
from typing import Iterator

from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC, T
from persisty.schema.schema_abc import SchemaABC
from persisty.store_schemas import StoreSchemas, schemas_for_type


@dataclass(frozen=True)
class SchemaStore(WrapperStoreABC[T]):
    """
    Wrapper which covers another and includes a search filter. Creates or update not matching the search filter
    fail outright. Effectively creates a partial view of another store (Useful for enforcing security constraints)
    """
    wrapped_store: StoreABC[T]
    op_schemas: StoreSchemas[T]

    @property
    def store(self) -> StoreABC[T]:
        return self.wrapped_store

    @property
    def schemas(self) -> StoreSchemas[T]:
        return self.op_schemas

    @property
    def name(self) -> str:
        return self.store.name

    def create(self, item: T) -> str:
        self._validate_item(item, self.schemas.create)
        return self.store.create(item)

    def update(self, item: T) -> T:
        self._validate_item(item, self.schemas.update)
        return self.store.update(item)

    def edit_all(self, edits: Iterator[Edit[T]]):
        """ Perform a bulk edit for items in this store. This action is not typically atomic. """
        edits = self._validate_edits(edits)
        self.store.edit_all(edits)

    def _validate_edits(self, edits: Iterator[Edit[T]]) -> Iterator[Edit[T]]:
        for edit in edits:
            if edit.edit_type == EditType.CREATE:
                self._validate_item(edit.item, self.schemas.create)
            if edit.edit_type == EditType.UPDATE:
                self._validate_item(edit.item, self.schemas.update)
            yield edit

    @staticmethod
    def _validate_item(item: T, schema: SchemaABC[T]):
        if schema is None:
            return
        error = next(schema.get_schema_errors(item), None)
        if error:
            raise error


def schema_store(store: StoreABC[T], key_attr: str = 'id') -> SchemaStore[T]:
    schemas = schemas_for_type(store.item_type, key_attr, store.capabilities)
    return SchemaStore(store, schemas)
