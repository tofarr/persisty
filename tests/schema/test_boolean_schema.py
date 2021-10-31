from unittest import TestCase

from marshy.default_context import new_default_context

from persisty.schema.boolean_schema import BooleanSchema
from persisty.schema.json_schema_abc import JsonSchemaABC
from persisty.schema.marshaller.json_schema_marshaller_factory import JsonSchemaMarshallerFactory
from persisty.schema.schema_error import SchemaError


class TestObjectSchema(TestCase):

    def test_boolean_schema(self):
        schema = BooleanSchema()
        # noinspection PyTypeChecker
        assert list(schema.get_schema_errors('True')) == [SchemaError('', 'type', 'True')]
        assert list(schema.get_schema_errors(True)) == []

    def test_marshalling(self):
        context = new_default_context()
        context.register_factory(JsonSchemaMarshallerFactory(priority=200))
        assert context.load(BooleanSchema, dict(type='boolean')) == BooleanSchema()
        assert context.load(JsonSchemaABC, dict(type='boolean')) == BooleanSchema()
        assert context.dump(BooleanSchema()) == dict(type='boolean')
