from typing import Union, List

from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.types import ExternalItemType

from persisty.schema.array_schema import ArraySchema
from persisty.schema.boolean_schema import BooleanSchema
from persisty.schema.json_schema_abc import JsonSchemaABC
from persisty.schema.marshaller.array_schema_marshaller import ArraySchemaMarshaller
from persisty.schema.marshaller.boolean_schema_marshaller import BooleanSchemaMarshaller
from persisty.schema.marshaller.number_schema_marshaller import NumberSchemaMarshaller
from persisty.schema.marshaller.object_schema_marshaller import ObjectSchemaMarshaller
from persisty.schema.marshaller.optional_schema_marshaller import OptionalSchemaMarshaller
from persisty.schema.marshaller.string_schema_marshaller import StringSchemaMarshaller
from persisty.schema.number_schema import NumberSchema
from persisty.schema.object_schema import ObjectSchema
from persisty.schema.optional_schema import OptionalSchema
from persisty.schema.string_schema import StringSchema


class JsonSchemaMarshaller(MarshallerABC[JsonSchemaABC]):

    """ Custom marshaller to json schema format """

    def load(self, item: ExternalItemType) -> JsonSchemaABC:
        item_type = item.get('type')
        marshaller = self.get_marshaller_by_name(item_type)
        schema = marshaller.load(item)
        return schema

    def get_marshaller_by_name(self, item_type: Union[str, List[str]]) -> MarshallerABC[JsonSchemaABC]:
        if isinstance(item_type, list):
            item_type = next(t for t in item_type if t is not None)
            marshaller = self.get_marshaller_by_name(item_type)
            return OptionalSchemaMarshaller(OptionalSchema, marshaller)
        elif item_type == 'array':
            return ArraySchemaMarshaller(ArraySchema, self)
        elif item_type == 'boolean':
            return BooleanSchemaMarshaller(BooleanSchema)
        elif item_type in ['integer', 'number']:
            return NumberSchemaMarshaller(NumberSchema)
        elif item_type == 'object':
            return ObjectSchemaMarshaller(ObjectSchema, self)
        elif item_type == 'string':
            return StringSchemaMarshaller(StringSchema)
        else:
            raise ValueError('unsupported_type:{item_type}')

    def dump(self, schema: JsonSchemaABC) -> ExternalItemType:
        marshaller = self.get_marshaller_by_schema(schema)
        dumped = marshaller.dump(schema)
        return dumped

    def get_marshaller_by_schema(self, schema: JsonSchemaABC) -> MarshallerABC[JsonSchemaABC]:
        if isinstance(schema, OptionalSchema):
            marshaller = self.get_marshaller_by_schema(schema.schema)
            return OptionalSchemaMarshaller(OptionalSchema, marshaller)
        elif isinstance(schema, ArraySchema):
            return ArraySchemaMarshaller(ArraySchema, self)
        elif isinstance(schema, BooleanSchema):
            return BooleanSchemaMarshaller(BooleanSchema)
        elif isinstance(schema, NumberSchema):
            return NumberSchemaMarshaller(NumberSchema)
        elif isinstance(schema, ObjectSchema):
            return ObjectSchemaMarshaller(ObjectSchema, self)
        elif isinstance(schema, StringSchema):
            return StringSchemaMarshaller(StringSchema)
        else:
            raise ValueError(f'unsupported_type:{schema}')
