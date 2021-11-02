from dataclasses import is_dataclass, fields
from typing import Type, TypeVar

import typing_inspect
from marshy.factory.optional_marshaller_factory import get_optional_type
from marshy.utils import resolve_forward_refs

from persisty.schema.schema_abc import SchemaABC, T

U = TypeVar('U')


def schema_for_type(type_: Type[T]) -> SchemaABC[T]:
    from persisty.schema.array_schema import ArraySchema
    from persisty.schema.boolean_schema import BooleanSchema
    from persisty.schema.json_schema_abc import JsonSchemaABC
    from persisty.schema.number_schema import NumberSchema
    from persisty.schema.object_schema import ObjectSchema
    from persisty.schema.optional_schema import OptionalSchema
    from persisty.schema.property_schema import PropertySchema
    from persisty.schema.string_schema import StringSchema
    """
    Attempts to generate a schema for the type given. Permits vagueness rather than throwing errors.
    """
    type_ = resolve_forward_refs(type_)
    if hasattr(type_, '__schema__'):
        return type_.__schema__
    optional_type: Type[U] = get_optional_type(type_)
    if optional_type is not None:
        schema = schema_for_type(optional_type)
        return None if schema is None else OptionalSchema[U](schema)
    origin = typing_inspect.get_origin(type_)
    args = typing_inspect.get_args(type_)
    if origin is list:
        item_schema: SchemaABC[U] = schema_for_type(args[0])
        return ArraySchema[T](item_schema)
    if type_ == str:
        return StringSchema()
    elif type_ in [int, float]:
        return NumberSchema(type_)
    elif type_ == bool:
        return BooleanSchema()
    elif is_dataclass(type_):
        property_schemas = []
        # noinspection PyDataclass
        for f in fields(type_):
            schema: SchemaABC[U] = f.metadata.get('schema')
            if not isinstance(schema, SchemaABC):
                schema = schema_for_type(f.type)
            if schema is not None:
                # noinspection PyTypeChecker
                property_schemas.append(PropertySchema(f.name, schema))
        return ObjectSchema(property_schemas)
