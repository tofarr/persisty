from dataclasses import dataclass, field, fields, is_dataclass
from typing import Iterable, Union, Sized, Optional, List, Iterator, Type

from marshy.factory.optional_marshaller_factory import get_optional_type

from persisty.schema.boolean_schema import BooleanSchema
from persisty.schema.json_schema_abc import JsonSchemaABC
from persisty.schema.number_schema import NumberSchema
from persisty.schema.optional_schema import OptionalSchema
from persisty.schema.property_schema import PropertySchema
from persisty.schema.string_schema import StringSchema
from persisty.schema.schema_error import SchemaError
from persisty.schema.schema_abc import T


@dataclass(frozen=True)
class ObjectSchema(JsonSchemaABC[T]):
    property_schemas: Union[Iterable[PropertySchema], Sized] = field(default_factory=list)

    def get_schema_errors(self, item: T, current_path: Optional[List[str]] = None) -> Iterator[SchemaError]:
        for property_validator in (self.property_schemas or []):
            yield from property_validator.get_schema_errors(item, current_path)


def schema_for_type(cls: Type[T]) -> ObjectSchema[T]:
    if hasattr(cls, '__validator__'):
        return cls.__validator__
    property_schemas = []
    for f in fields(cls):
        schema = f.metadata.get('schema')
        if isinstance(schema, JsonSchemaABC):
            property_schemas.append(PropertySchema(f.name, schema))
            continue
        field_type = f.type
        optional = False
        schema = None
        optional_type = get_optional_type(f.type)
        if optional_type:
            field_type = optional_type
            optional = True
        if field_type == str:
            schema = StringSchema()
        elif field_type in [int, float]:
            schema = NumberSchema(field_type)
        elif field_type == bool:
            schema = BooleanSchema()
        elif is_dataclass(field_type):
            schema = schema_for_type(field_type)
        if optional and schema:
            schema = OptionalSchema(schema)
        if schema:
            property_schemas.append(PropertySchema(f.name, schema))
    return ObjectSchema(property_schemas)
