from dataclasses import dataclass
from typing import TypeVar, Type

from persisty.errors import PersistyError
from schemey.any_of_schema import strip_optional
from schemey.boolean_schema import BooleanSchema
from schemey.number_schema import NumberSchema
from schemey.object_schema import ObjectSchema
from schemey.schema_abc import SchemaABC
from schemey.schema_context import schema_for_type
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


def col_for_property(name: str, schema: SchemaABC):
    stripped_schema = strip_optional(schema)
    not_null = stripped_schema is schema
    if isinstance(stripped_schema, NumberSchema):
        if stripped_schema.item_type == int:
            return SqlCol(name, not_null, 'INT')
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
