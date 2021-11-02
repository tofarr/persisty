from dataclasses import dataclass

from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.types import ExternalItemType

from persisty.schema.boolean_schema import BooleanSchema
from persisty.schema.json_schema_abc import JsonSchemaABC
from persisty.schema.optional_schema import OptionalSchema


@dataclass(frozen=True)
class OptionalSchemaMarshaller(MarshallerABC[OptionalSchema]):
    marshaller: MarshallerABC[JsonSchemaABC]

    def load(self, item: ExternalItemType) -> OptionalSchema:
        item_type = next(t for t in item.get('type') if t is not None)
        item = {**item, 'type': item_type}
        schema = self.marshaller.load(item)
        return OptionalSchema(schema)

    def dump(self, item: OptionalSchema) -> ExternalItemType:
        item = self.marshaller.dump(item.schema)
        item = {**item, 'type': [item['type'], None]}
        return item
