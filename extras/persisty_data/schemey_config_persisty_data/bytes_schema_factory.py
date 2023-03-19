from typing import Dict, Optional, Type

from marshy.types import ExternalItemType
from schemey import SchemaContext, Schema
from schemey.factory.schema_factory_abc import SchemaFactoryABC


class BytesSchemaFactory(SchemaFactoryABC):
    priority: int = 110

    def from_type(
        self,
        type_: Type,
        context: SchemaContext,
        path: str,
        ref_schemas: Dict[Type, Schema],
    ) -> Optional[Schema]:
        if type_ is bytes:
            return Schema({"type": "string", "contentEncoding": "base64"}, bytes)

    def from_json(
        self,
        item: ExternalItemType,
        context: SchemaContext,
        path: str,
        ref_schemas: Dict[str, Schema],
    ) -> Optional[Schema]:
        if item.get("type") == "string" and item.get("contentEncoding") == "base64":
            return Schema(item, bytes)
