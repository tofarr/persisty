from unittest import TestCase

from persisty.schema.number_schema import NumberSchema
from persisty.schema.object_schema import schema_for_type, ObjectSchema
from persisty.schema.optional_schema import OptionalSchema
from persisty.schema.property_schema import PropertySchema
from persisty.schema.schema_error import SchemaError
from persisty.schema.string_schema import StringSchema
from tests.fixtures.items import Band


class TestObjectSchema(TestCase):

    def test_object_schema(self):
        schema = ObjectSchema([
            PropertySchema('id', StringSchema(min_length=1)),
            PropertySchema('year_formed', OptionalSchema(NumberSchema(int, minimum=1900))),
        ])
        assert list(schema.get_schema_errors(Band())) == [SchemaError('id', 'type')]
        assert list(schema.get_schema_errors(Band(''))) == [SchemaError('id', 'min_length', '')]
        assert list(schema.get_schema_errors(Band('mozart', 'Mozart', 1756))) == \
               [SchemaError('year_formed', 'minimum', 1756)]

    def test_schema_for_type(self):
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
