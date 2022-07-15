import dataclasses
from datetime import datetime
from typing import Type, TypeVar, Union, get_type_hints, Optional, Tuple, Generic
from uuid import UUID

from marshy.factory.optional_marshaller_factory import get_optional_type
from schemey.schema_abc import SchemaABC
from schemey.schema_context import schema_for_type
from schemey.string_format import StringFormat
from schemey.string_schema import StringSchema

from persisty.item.generator.default_value_generator import DefaultValueGenerator
from persisty.item.generator.generator_abc import GeneratorABC
from persisty.item.generator.null_generator import NULL_GENERATOR
from persisty.item.generator.timestamp_generator import CREATED_AT_GENERATOR, UPDATED_AT_GENERATOR
from persisty.item.generator.uuid_generator import UUID_ALWAYS_ON_CREATE
from persisty.item.security.field_access_control import ALL_ACCESS, READ_ONLY
from persisty.item.security.field_access_control_abc import FieldAccessControlABC
from persisty.util.undefined import Undefined, UNDEFINED

T = TypeVar('T')


@dataclasses.dataclass(frozen=True)
class Field(Generic[T]):
    name: str = None
    description: str = None
    type: Type[T] = None
    schema: SchemaABC[T] = None
    generator: Optional[GeneratorABC[T]] = None
    access_control: FieldAccessControlABC = ALL_ACCESS
    indexed: bool = False  # This is a hint only - depending on the impl the field may not actually be indexed

    def __post_init__(self):
        if get_optional_type(self.type) and self.generator is None:
            object.__setattr__(self, 'generator', NULL_GENERATOR)
        if self.type and not self.schema:
            object.__setattr__(self, 'schema', schema_for_type(self.type))

    def __set_name__(self, owner, name):
        object.__setattr__(self, 'name', name)
        type_hints = get_type_hints(owner)
        hint_type = type_hints.get(name)
        if self.type and hint_type and hint_type != self.type:
            raise ValueError(f'incorrect_annotation:{owner.__class__.__name__}:{self.name}')
        elif not self.type and not hint_type:
            raise ValueError(f'missing_annotation:{owner.__class__.__name__}:{self.name}')
        if hint_type:
            object.__setattr__(self, 'type', hint_type)
        self.__post_init__()

    def __get__(self, instance, owner) -> Union[T, Undefined]:
        value = instance.__dict__.get(self.name, UNDEFINED)
        return value

    def __set__(self, instance, value: T):
        self.schema.validate(value, [self.name])
        instance.__dict__[self.name] = value


def fields_for_type(type_: Type) -> Tuple[Field, ...]:
    if not hasattr(type_, '__persisty_fields__'):
        raise ValueError(f'not_an_item:{type_}')
    return getattr(type_, '__persisty_fields__')


def uuid_field(
    name: Optional[str] = 'id',
    description: Optional[str] = None,
    access_control: FieldAccessControlABC = READ_ONLY,  # uuids are usually used for immutable ids
    generator: Union[Undefined, type(None), GeneratorABC[T]] = UNDEFINED,
) -> Field[T, UUID]:
    if generator is UNDEFINED:
        if name == 'id':
            generator = UUID_ALWAYS_ON_CREATE
        else:
            generator = None
    return Field(
        name=name,
        description=description,
        type=UUID,
        access_control=access_control,
        generator=generator,
        indexed=True
    )


def created_at_field(
    name: Optional[str] = 'created_at',
    access_control: FieldAccessControlABC = READ_ONLY
):
    return Field(
        name=name,
        type=datetime,
        access_control=access_control,
        generator=CREATED_AT_GENERATOR,
        indexed=True
    )


def updated_at_field(
    name: Optional[str] = 'created_at',
    access_control: FieldAccessControlABC = READ_ONLY,
):
    return Field(
        name=name,
        type=datetime,
        access_control=access_control,
        generator=UPDATED_AT_GENERATOR,
        indexed=True
    )


# noinspection PyShadowingBuiltins
def str_field(
    name: str = None,
    description: Optional[str] = None,
    access_control: FieldAccessControlABC = ALL_ACCESS,
    max_length: Optional[int] = 255,
    format: Optional[StringFormat] = None,
    pattern: Optional[str] = None,
    default_value: Union[Undefined, type(None), str] = None,
    indexed: bool = False
) -> Field:
    schema = StringSchema(max_length=max_length, format=format, pattern=pattern)
    generator = None
    type_ = str
    if default_value is not UNDEFINED:
        generator = DefaultValueGenerator(default_value)
        if default_value is None:
            type_ = Optional[str]
    return Field(
        name=name,
        description=description,
        type=type_,
        access_control=access_control,
        schema=schema,
        generator=generator,
        indexed=indexed
    )
