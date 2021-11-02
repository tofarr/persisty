from unittest import TestCase

from marshy.default_context import new_default_context

from persisty.schema.marshaller.json_schema_marshaller_factory import JsonSchemaMarshallerFactory
from persisty.schema.number_schema import NumberSchema
from persisty.schema.optional_schema import OptionalSchema
from persisty.schema.schema_error import SchemaError
from persisty.schema.string_schema import StringSchema


class TestOptionalSchema(TestCase):

    def test_optional_schema(self):
        schema = OptionalSchema(StringSchema())
        assert list(schema.get_schema_errors('foo')) == []
        assert list(schema.get_schema_errors(None)) == []
        assert list(schema.get_schema_errors(10)) == [SchemaError('', 'type', 10)]
        assert list(schema.get_schema_errors(10, ['foo', 'bar'])) == [SchemaError('foo/bar', 'type', 10)]

    def test_marshalling(self):
        context = new_default_context()
        context.register_factory(JsonSchemaMarshallerFactory(priority=200))
        assert context.load(OptionalSchema, dict(type=['integer', None])) == OptionalSchema(NumberSchema(item_type=int))
        assert context.dump(OptionalSchema(NumberSchema(item_type=float))) == dict(type=['number', None])
