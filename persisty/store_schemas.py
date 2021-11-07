import dataclasses
from dataclasses import dataclass
from typing import TypeVar, Generic, Type, ForwardRef, Optional

from persisty.capabilities import Capabilities, ALL_CAPABILITIES
from schemey.any_of_schema import strip_optional, optional_schema
from schemey.object_schema import ObjectSchema
from schemey.property_schema import PropertySchema
from schemey.schema_context import schema_for_type
from schemey.string_schema import StringSchema

T = TypeVar('T')


@dataclass(frozen=True)
class StoreSchemas(Generic[T]):
    create: ForwardRef('persisty.schema.SchemaABC[T]') = None
    update: ForwardRef('persisty.schema.SchemaABC[T]') = None
    read: ForwardRef('persisty.schema.SchemaABC[T]') = None
    search: ForwardRef('persisty.schema.SchemaABC[T]') = None


NO_SCHEMAS = StoreSchemas()


def schemas_for_type(type_: Type[T], key_attr: Optional[str] = 'id', capabilities: Capabilities = ALL_CAPABILITIES):
    schema = schema_for_type(type_)
    if not isinstance(schema, ObjectSchema):
        return StoreSchemas[T](schema, schema, schema)
    key_schema = next((s.schema for s in schema.property_schemas if s.name == key_attr), None)
    if not key_schema:
        return StoreSchemas[T](schema, schema, schema)
    key_schema = strip_optional(key_schema)
    if isinstance(key_schema, StringSchema) and not key_schema.min_length:
        key_schema = dataclasses.replace(key_schema, min_length=1)

    create = None
    if capabilities.create:
        create_properties = []
        if capabilities.create_with_key and key_schema:
            create_properties.append(PropertySchema(key_attr, optional_schema(key_schema)))
        create_properties.extend(s for s in schema.property_schemas if s.name != key_attr)
        create = ObjectSchema(tuple(create_properties))

    update = None
    if capabilities.update:
        update_properties = [PropertySchema(key_attr, key_schema)]
        update_properties.extend(s for s in schema.property_schemas if s.name != key_attr)
        update = ObjectSchema(tuple(update_properties))

    read = None
    if capabilities.read:
        read_properties = [PropertySchema(key_attr, key_schema)]
        read_properties.extend(s for s in schema.property_schemas if s.name != key_attr)
        read = ObjectSchema(tuple(read_properties))

    search = None
    if capabilities.search:
        read_properties = [PropertySchema(key_attr, key_schema)]
        read_properties.extend(s for s in schema.property_schemas if s.name != key_attr)
        search = ObjectSchema(tuple(read_properties))

    return StoreSchemas[T](
        create=create,
        update=update,
        read=read,
        search=search
    )