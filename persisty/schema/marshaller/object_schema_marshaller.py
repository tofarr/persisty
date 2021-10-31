from dataclasses import dataclass

from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.types import ExternalItemType

from persisty.schema.json_schema_abc import JsonSchemaABC
from persisty.schema.object_schema import ObjectSchema
from persisty.schema.property_schema import PropertySchema


@dataclass(frozen=True)
class ObjectSchemaMarshaller(MarshallerABC[ObjectSchema]):
    property_schema_marshaller: MarshallerABC[JsonSchemaABC]

    def load(self, item: ExternalItemType) -> ObjectSchema:
        return ObjectSchema(tuple(PropertySchema(k, self.property_schema_marshaller.load(v)) for k, v in item.items()))

    def dump(self, schema: ObjectSchema) -> ExternalItemType:
        item = {s.name: self.property_schema_marshaller.dump(s.schema) for s in schema.property_schemas}
        return item
