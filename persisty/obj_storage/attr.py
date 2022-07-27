from dataclasses import dataclass, fields
from datetime import datetime
from typing import Optional, Tuple, Type, get_type_hints, Union
from uuid import UUID

from schemey.schema_abc import SchemaABC
from schemey.schema_context import schema_for_type
from schemey.string_schema import StringSchema

from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.field.field import Field
from persisty.field.field_filter import FieldFilterOp
from persisty.field.field_type import FieldType
from persisty.field.write_transform.default_value_transform import (
    DefaultValueTransform,
)
from persisty.field.write_transform.int_sequence_generator import (
    IntSequenceGenerator,
)
from persisty.field.write_transform.str_sequence_genrator import (
    StrSequenceGenerator,
)
from persisty.field.write_transform.timestamp_generator import (
    TimestampGenerator,
)
from persisty.field.write_transform.uuid_generator import (
    UUID_OPTIONAL_ON_CREATE,
)
from persisty.field.write_transform.write_transform_abc import WriteTransformABC
from persisty.field.write_transform.write_transform_mode import (
    WriteTransformMode,
)
from persisty.util import UNDEFINED
from persisty.util.undefined import Undefined

FILTER_OPS = (FieldFilterOp.eq, FieldFilterOp.ne)
SORTABLE_FILTER_OPS = FILTER_OPS + (
    FieldFilterOp.gt,
    FieldFilterOp.gte,
    FieldFilterOp.lt,
    FieldFilterOp.lte,
)
STRING_FILTER_OPS = SORTABLE_FILTER_OPS + (
    FieldFilterOp.startswith,
    FieldFilterOp.endswith,
    FieldFilterOp.contains,
)
TYPE_MAP = {
    bool: FieldType.BOOL,
    datetime: FieldType.DATETIME,
    float: FieldType.FLOAT,
    int: FieldType.INT,
    str: FieldType.STR,
    UUID: FieldType.UUID,
}
TYPE_FILTER_OPS = {
    FieldType.BOOL: FILTER_OPS,
    FieldType.DATETIME: SORTABLE_FILTER_OPS,
    FieldType.FLOAT: SORTABLE_FILTER_OPS,
    FieldType.INT: SORTABLE_FILTER_OPS,
    FieldType.JSON: FILTER_OPS,
    FieldType.STR: STRING_FILTER_OPS,
    FieldType.UUID: FILTER_OPS,
}
NONE_TYPE = type(None)


@dataclass
class Attr:
    """
    Attributes contain all the data needed to create a Field instance - the primary Column / Attribute type used
    by persisty. This provides a bridge between internal and external attribute definitions
    """

    name: Optional[str] = None
    type: Optional[Type] = None
    field_type: Optional[FieldType] = None
    schema: Optional[SchemaABC] = None
    is_readable: bool = True
    is_creatable: Optional[bool] = None
    is_updatable: Optional[bool] = None
    write_transform: Union[WriteTransformABC, NONE_TYPE, Undefined] = UNDEFINED
    permitted_filter_ops: Optional[Tuple[FieldFilterOp, ...]] = None
    is_sortable: Optional[bool] = None
    description: Optional[str] = None
    is_indexed: Optional[bool] = None
    is_nullable: Optional[bool] = None

    def __set_name__(self, owner, name):
        self.name = name
        type_hints = get_type_hints(owner)
        hint_type = type_hints.get(name)
        if self.type and hint_type and hint_type != self.type:
            raise ValueError(
                f"incorrect_annotation:{owner.__class__.__name__}:{self.name}"
            )
        elif not self.type and not hint_type:
            raise ValueError(
                f"missing_annotation:{owner.__class__.__name__}:{self.name}"
            )
        if hint_type:
            self.type = hint_type

    def __get__(self, instance, owner):
        value = instance.__dict__.get(self.name, UNDEFINED)
        return value

    def __set__(self, instance, value):
        self.schema.validate(value, [self.name])
        instance.__dict__[self.name] = value

    def to_field(self) -> Field:
        kwargs = {f.name: getattr(self, f.name) for f in fields(Field)}
        kwargs["type"] = self.field_type
        return Field(**kwargs)

    def populate(self, key_config: KeyConfigABC):
        self.populate_field_type()
        self.populate_schema()
        self.populate_write_transform()
        self.populate_nullable(key_config)
        self.populate_permitted_filter_ops()
        self.populate_sortable()
        self.populate_indexed(key_config)
        self.populate_access()

    def populate_field_type(self):
        if self.field_type is not None:
            return
        # noinspection PyTypeChecker
        self.field_type = TYPE_MAP.get(self.type, FieldType.JSON)

    def populate_schema(self):
        if self.schema is not None:
            return
        elif self.type is str:
            self.schema = StringSchema(max_length=255)
        else:
            self.schema = schema_for_type(self.type).json_schema

    def populate_write_transform(self):
        if self.write_transform is not UNDEFINED:
            return
        if self.name == "id":
            if self.field_type == FieldType.UUID:
                self.write_transform = UUID_OPTIONAL_ON_CREATE
            elif self.field_type == FieldType.INT:
                self.write_transform = IntSequenceGenerator()
            elif self.field_type == FieldType.STR:
                self.write_transform = StrSequenceGenerator()
        elif self.name == "created_at" and self.field_type == FieldType.DATETIME:
            self.write_transform = TimestampGenerator(False)
        elif self.name == "updated_at" and self.field_type == FieldType.DATETIME:
            self.write_transform = TimestampGenerator(True)
        elif self.is_nullable:
            self.write_transform = DefaultValueTransform(None)
        else:
            self.write_transform = None

    def populate_nullable(self, key_config: KeyConfigABC):
        if self.is_nullable is None:
            if key_config.is_required_field(self.name):
                self.is_nullable = False
            elif not self.write_transform:
                self.is_nullable = True

    def populate_permitted_filter_ops(self):
        if self.permitted_filter_ops is None:
            self.permitted_filter_ops = TYPE_FILTER_OPS[self.field_type]

    def populate_sortable(self):
        if self.is_sortable is None:
            self.is_sortable = FieldFilterOp.lt in self.permitted_filter_ops

    def populate_indexed(self, key_config: KeyConfigABC):
        if self.is_indexed is None:
            self.is_indexed = (
                key_config.is_required_field(self.name)
                or "id" in self.name
                or "code" in self.name
            )

    def populate_access(self):
        t = self.write_transform
        if self.is_creatable is None:
            if t and t.mode in (
                WriteTransformMode.ALWAYS_FOR_CREATE,
                WriteTransformMode.ALWAYS_FOR_WRITE,
            ):
                self.is_creatable = False
            else:
                self.is_creatable = True
        if self.is_updatable is None:
            if t and t.mode in (
                WriteTransformMode.ALWAYS_FOR_UPDATE,
                WriteTransformMode.ALWAYS_FOR_WRITE,
            ):
                self.is_updatable = False
            else:
                self.is_updatable = True
