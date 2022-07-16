import dataclasses
from datetime import datetime
from typing import Any, Type, Union
from uuid import UUID

from schemey.schema_context import schema_for_type

from persisty.item.field import Field, str_field, uuid_field, created_at_field, updated_at_field
from persisty.item.generator.default_value_generator import DefaultValueGenerator
from persisty.util.undefined import UNDEFINED, Undefined


def item(cls):
    fields = {}
    annotations = cls.__dict__['__annotations__']
    for name, field in cls.__dict__.items():
        if name.startswith('__'):
            continue
        if not isinstance(field, Field):
            # sometimes a value may be provided instead of a field, so we wrap these...
            type_ = annotations[name]
            field = create_field(name, type_, field)
        fields[name] = field

    # Add fields for annotations which don't have values...
    for name, type_ in annotations.items():
        if name not in fields:
            fields[name] = create_field(name, type_)

    attrs = {name: UNDEFINED for name in fields}
    annotations = {name: Union[Undefined, type_] for name, type_ in annotations.items()}
    attrs.update({'__annotations__': annotations, '__persisty_fields__': tuple(fields.values())})
    wrapped = type(cls.__name__, tuple(), attrs)
    wrapped = dataclasses.dataclass(wrapped)
    return wrapped


def create_field(name: str, type_: Type, default_value: Any = None) -> Field:
    if isinstance(default_value, dataclasses.Field):
        return create_field(name, type_, default_value.default)
    if isinstance(type_, str):
        return str_field(name=name, default_value=default_value)
    if isinstance(type_, UUID) and default_value in (None, UNDEFINED, dataclasses.MISSING):
        return uuid_field(name=name)
    if isinstance(type_, datetime) and default_value in (None, UNDEFINED, dataclasses.MISSING):
        if name == 'created_at':
            return created_at_field()
        if name == 'updated_at':
            return updated_at_field()
    schema = schema_for_type(type_)
    generator = None
    if default_value not in (UNDEFINED, dataclasses.MISSING):
        generator = DefaultValueGenerator(default_value)
    return Field(name=name, type=type_, generator=generator, schema=schema)
