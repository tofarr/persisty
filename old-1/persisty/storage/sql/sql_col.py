from dataclasses import dataclass
from datetime import datetime
from typing import TypeVar, Type, Optional

from schemey.datetime_schema import DatetimeSchema

from persisty.attr.attr import Attr
from persisty.attr.attr_access_control import REQUIRED, OPTIONAL
from persisty.errors import PersistyError
from schemey.any_of_schema import strip_optional, optional_schema
from schemey.boolean_schema import BooleanSchema
from schemey.number_schema import NumberSchema
from schemey.object_schema import ObjectSchema
from schemey.schema_abc import SchemaABC
from schemey.schema_context import schema_for_type, get_default_schema_context, SchemaContext
from schemey.string_format import StringFormat
from schemey.string_schema import StringSchema

T = TypeVar('T')


@dataclass(frozen=True)
class SqlCol:
    name: str
    not_null: bool
    sql_type: str

    def to_sql(self, primary_key: bool = False, auto_increment: bool = False):
        sql = f'{self.name} {self.sql_type}'
        if self.not_null:
            sql += ' NOT NULL'
        if primary_key:
            sql += ' PRIMARY KEY'
        if auto_increment:
            sql += ' AUTOINCREMENT'
        return sql

    def to_attr(self):
        if self.sql_type.startswith('VARCHAR('):
            schema = StringSchema(max_length=int(''.join(s for s in self.sql_type.split() if s.isdigit())))
        elif self.sql_type == 'INTEGER':
            schema = NumberSchema(int)
        elif self.sql_type == 'FLOAT':
            schema = NumberSchema(float)
        elif self.sql_type == 'BOOLEAN':
            schema = BooleanSchema()
        elif self.sql_type == 'DATETIME':
            schema = DatetimeSchema()
        elif self.sql_type == 'TEXT':
            schema = StringSchema()
        else:
            raise ValueError(f'unsuported_type:{self.sql_type}')
        if not self.not_null:
            schema = optional_schema(schema)
        return Attr(name=self.name, schema=schema)


def col_for_property(name: str, schema: SchemaABC):
    stripped_schema = strip_optional(schema)
    not_null = stripped_schema is schema
    if isinstance(stripped_schema, NumberSchema):
        if stripped_schema.item_type == int:
            return SqlCol(name, not_null, 'INTEGER')
        elif stripped_schema.item_type == datetime:
            return SqlCol(name, not_null, 'DATETIME')
        return SqlCol(name, not_null, 'FLOAT')
    if isinstance(stripped_schema, BooleanSchema):
        return SqlCol(name, not_null, 'BOOLEAN')
    if isinstance(stripped_schema, StringSchema):
        if stripped_schema.format == StringFormat.DATE_TIME:
            return SqlCol(name, not_null, 'DATETIME')
        if stripped_schema.max_length and stripped_schema.max_length < 256:
            return SqlCol(name, not_null, f'VARCHAR({stripped_schema.max_length})')
    return SqlCol(name, not_null, 'TEXT')


def cols_for_type(item_type: Type):
    # noinspection PyTypeChecker
    schema = schema_for_type(item_type)
    if not isinstance(schema, ObjectSchema):
        raise PersistyError(f'unknown_type:{schema}')
    for p in schema.property_schemas:
        col = col_for_property(p.name, p.schema)
        yield col
