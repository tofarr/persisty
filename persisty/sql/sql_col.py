from dataclasses import dataclass
from typing import TypeVar, Type

from persisty.errors import PersistyError
from persisty.schema import SchemaABC, schema_for_type
from persisty.schema.boolean_schema import BooleanSchema
from persisty.schema.number_schema import NumberSchema
from persisty.schema.object_schema import ObjectSchema
from persisty.schema.optional_schema import OptionalSchema
from persisty.schema.string_format import StringFormat
from persisty.schema.string_schema import StringSchema

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
    not_null = True
    if isinstance(schema, OptionalSchema):
        not_null = False
        schema = schema.schema
    if isinstance(schema, NumberSchema):
        if schema.item_type == int:
            return SqlCol(name, not_null, 'INT')
        return SqlCol(name, not_null, 'FLOAT')
    if isinstance(schema, BooleanSchema):
        return SqlCol(name, not_null, 'BOOLEAN')
    if isinstance(schema, StringSchema):
        if schema.format == StringFormat.DATE_TIME:
            return SqlCol(name, not_null, 'DATETIME')
        if schema.max_length and schema.max_length < 256:
            return SqlCol(name, not_null, f'VARCHAR({schema.max_length})')
    return SqlCol(name, not_null, 'TEXT')


def cols_for_type(item_type: Type):
    # noinspection PyTypeChecker
    schema = schema_for_type(item_type)
    if not isinstance(schema, ObjectSchema):
        raise PersistyError(f'unknown_type:{schema}')
    for p in schema.property_schemas:
        col = col_for_property(p.name, p.schema)
        yield col
