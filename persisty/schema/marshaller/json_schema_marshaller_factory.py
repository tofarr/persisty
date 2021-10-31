from typing import Type, Optional

from marshy.factory.marshaller_factory_abc import MarshallerFactoryABC
from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.marshaller_context import MarshallerContext

from persisty.schema.json_schema_abc import JsonSchemaABC
from persisty.schema.marshaller.json_schema_marshaller import JsonSchemaMarshaller


class JsonSchemaMarshallerFactory(MarshallerFactoryABC):

    def create(self, context: MarshallerContext, type_: Type) -> Optional[MarshallerABC]:
        if type_ == JsonSchemaABC or issubclass(type_, JsonSchemaABC):
            return JsonSchemaMarshaller(JsonSchemaABC)
