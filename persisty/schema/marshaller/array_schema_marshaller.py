from dataclasses import dataclass

from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.types import ExternalItemType

from persisty.util import filter_none
from persisty.schema.array_schema import ArraySchema
from persisty.schema.json_schema_abc import JsonSchemaABC


@dataclass(frozen=True)
class ArraySchemaMarshaller(MarshallerABC[ArraySchema]):
    item_schema_marshaller: MarshallerABC[JsonSchemaABC]

    def load(self, item: ExternalItemType) -> ArraySchema:
        item_schema = self.item_schema_marshaller.load(item['items']) if 'items' in item else None
        return ArraySchema(
            item_schema=item_schema,
            min_items=int(item['minItems']) if hasattr(item, 'minItems') else None,
            max_items=int(item['maxItems']) if hasattr(item, 'maxItems') else None,
            uniqueness=item.get('uniqueness') is True
        )

    def dump(self, schema: ArraySchema) -> ExternalItemType:
        return filter_none(dict(
            item=self.item_schema_marshaller.dump(schema.item_schema) if schema.item_schema else None,
            minItems=schema.min_items,
            maxItems=schema.max_items,
            uniqueness=schema.uniqueness
        ))
