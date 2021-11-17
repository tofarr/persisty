from dataclasses import dataclass
from typing import Iterator, Type, Iterable

from schemey.any_of_schema import strip_optional, optional_schema
from schemey.object_schema import ObjectSchema
from schemey.property_schema import PropertySchema
from schemey.schema_abc import SchemaABC

from persisty.attr.attr import Attr
from persisty.attr.attr_mode import AttrMode
from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.storage.storage_abc import StorageABC
from persisty.storage.wrappers.wrapper_storage_abc import WrapperStorageABC, T


@dataclass(frozen=True)
class SchemaValidatedStorage(WrapperStorageABC[T]):
    """
    Wrapper which covers another and includes a search filter. Creates or update not matching the search filter
    fail outright. Effectively creates a partial view of another storage (Useful for enforcing security constraints)
    """
    wrapped_storage: StorageABC[T]
    schema_for_create: SchemaABC[T]
    schema_for_update: SchemaABC[T]

    @property
    def storage(self) -> StorageABC[T]:
        return self.wrapped_storage

    def create(self, item: T) -> str:
        self.schema_for_create.validate(item)
        return self.storage.create(item)

    def update(self, item: T) -> T:
        self.schema_for_update.validate(item)
        return self.storage.update(item)

    def edit_all(self, edits: Iterator[Edit[T]]):
        edits = self._validate_edits(edits)
        self.storage.edit_all(edits)

    def _validate_edits(self, edits: Iterator[Edit[T]]) -> Iterator[Edit[T]]:
        for edit in edits:
            if edit.edit_type == EditType.CREATE:
                self.schema_for_create.validate(edit.item)
            if edit.edit_type == EditType.UPDATE:
                self.schema_for_update.validate(edit.item)
            yield edit


def schema_validated_storage(storage: StorageABC[T]) -> SchemaValidatedStorage[T]:
    """ Wrap the storage given and make sure that validation rules are being enforced """
    meta = storage.meta
    create_schema = _get_schema_for_mode(storage.item_type, meta.attrs, 'create_mode')
    update_schema = _get_schema_for_mode(storage.item_type, meta.attrs, 'update_mode')
    return SchemaValidatedStorage(storage, create_schema, update_schema)


def _get_schema_for_mode(item_type: Type, attrs: Iterable[Attr], mode: str):
    property_schemas = tuple(_generate_property_schemas_for_mode(attrs, mode))
    return ObjectSchema[item_type](item_type, property_schemas)


def _generate_property_schemas_for_mode(attrs: Iterable[Attr], mode: str) -> Iterator[PropertySchema]:
    for attr in attrs:
        m = getattr(attr.attr_access_control, mode)
        schema = strip_optional(attr.schema)
        if m == AttrMode.REQUIRED:
            yield PropertySchema(attr.name, schema, True)
        elif m == AttrMode.OPTIONAL:
            yield PropertySchema(attr.name, optional_schema(schema))
