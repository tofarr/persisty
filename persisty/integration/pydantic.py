from typing import TypeVar, Union, Optional, Callable, Type

from pydantic import BaseModel
from schemey import Schema

from persisty.field.field import Field
from persisty.obj_storage.stored import get_storage_meta
from persisty.storage.storage_meta import StorageMeta
from persisty.util import to_camel_case

T = TypeVar("T")


def stored_to_pydantic_model(
    stored: T, field_check: Optional[Callable[[Field], bool]] = None
) -> Union[T, BaseModel]:
    """Convert an entity annotated with @stored to a pydantic model"""
    storage_meta = get_storage_meta(stored)
    return storage_meta_to_pydantic_model(storage_meta, field_check)


def storage_meta_to_pydantic_model(
    storage_meta: StorageMeta,
    field_check: Optional[Callable[[Field], bool]] = None,
    name: Optional[str] = None,
):
    """Build a pydantic model from the storage_meta given"""
    if field_check is None:
        field_check = _is_readable
    if not name:
        name = to_camel_case(storage_meta.name)
    annotations = {
        f.name: field_for_pydantic(f.schema)
        for f in storage_meta.fields
        if field_check(f)
    }
    type_ = type(name, (BaseModel,), dict(__annotations__=annotations))
    return type_


def _is_readable(field: Field) -> bool:
    return field.is_readable


def field_for_pydantic(schema: Schema) -> Type:
    class FieldForPydantic:
        @classmethod
        def __get_validators__(cls):
            yield cls.validate

        @classmethod
        def __modify_schema__(cls, field_schema):
            field_schema.update(schema.schema)

        @classmethod
        def validate(cls, v):
            schema.validate(v)
            return v

        def __repr__(self):
            return f"FieldForPydantic({schema.__repr__()})"

    return FieldForPydantic
