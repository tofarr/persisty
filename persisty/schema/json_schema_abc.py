from abc import ABC

from persisty.schema.schema_abc import SchemaABC, T


class JsonSchemaABC(SchemaABC[T], ABC):
    pass
