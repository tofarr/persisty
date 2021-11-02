from dataclasses import dataclass, field
from typing import List, ForwardRef, Optional, Iterator
from unittest import TestCase

from marshy.default_context import new_default_context

from persisty.schema import schema_for_type
from persisty.schema.array_schema import ArraySchema
from persisty.schema.boolean_schema import BooleanSchema
from persisty.schema.json_schema_abc import JsonSchemaABC, T
from persisty.schema.marshaller.json_schema_marshaller_factory import JsonSchemaMarshallerFactory
from persisty.schema.number_schema import NumberSchema
from persisty.schema.object_schema import ObjectSchema
from persisty.schema.optional_schema import OptionalSchema
from persisty.schema.property_schema import PropertySchema
from persisty.schema.schema_error import SchemaError
from persisty.schema.string_schema import StringSchema
from tests.fixtures.items import Band


@dataclass
class Node:
    id: str
    tags: List[str] = field(default_factory=list)
    status: ForwardRef(f'{__name__}.Status') = None


@dataclass
class Status:
    title: str
    public: bool = False


@dataclass
class DefinesSchema:
    __schema__ = ObjectSchema([
        PropertySchema('some_bool', BooleanSchema())
    ])


class TestSchema(TestCase):

    def test_schema_for_type_band(self):
        schema = schema_for_type(Band)
        expected = ObjectSchema([
            PropertySchema('id', OptionalSchema(StringSchema())),
            PropertySchema('band_name', OptionalSchema(StringSchema())),
            PropertySchema('year_formed', OptionalSchema(NumberSchema(int))),
        ])
        assert expected == schema
        assert not list(schema.get_schema_errors(Band()))
        # noinspection PyTypeChecker
        assert len(list(schema.get_schema_errors(Band(23)))) == 1
        # noinspection PyTypeChecker
        assert len(list(schema.get_schema_errors(Band(23, False)))) == 2

    def test_schema_for_type_node(self):
        schema = schema_for_type(Node)
        expected = ObjectSchema([
            PropertySchema('id', StringSchema()),
            PropertySchema('tags', ArraySchema(StringSchema())),
            PropertySchema('status', ObjectSchema([
                PropertySchema('title', StringSchema()),
                PropertySchema('public', BooleanSchema())
            ]))
        ])
        assert expected == schema

    def test_schema(self):
        schema = schema_for_type(DefinesSchema)
        assert schema == DefinesSchema.__schema__

    def test_load_invalid(self):
        context = new_default_context()
        context.register_factory(JsonSchemaMarshallerFactory(priority=200))
        with self.assertRaises(ValueError):
            context.load(JsonSchemaABC, dict(type='unknown'))

    def test_store_invalid(self):
        class WeirdSchema(JsonSchemaABC):

            def get_schema_errors(self, item: T, current_path: Optional[List[str]] = None) -> Iterator[SchemaError]:
                """ Never actually called"""

        context = new_default_context()
        context.register_factory(JsonSchemaMarshallerFactory(priority=200))
        with self.assertRaises(ValueError):
            context.dump(WeirdSchema())
        with self.assertRaises(ValueError):
            context.dump(dict(type='weird'), JsonSchemaABC)
