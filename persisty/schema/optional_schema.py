from dataclasses import dataclass
from typing import TypeVar, Iterator, Optional, List

from persisty.schema import SchemaABC
from persisty.schema.json_schema_abc import JsonSchemaABC
from persisty.schema.schema_error import SchemaError

T = TypeVar('T')


@dataclass(frozen=True)
class OptionalSchema(JsonSchemaABC[T]):
    schema: JsonSchemaABC[T]

    def get_schema_errors(self,
                          item: Optional[T],
                          current_path: Optional[List[str]] = None
                          ) -> Iterator[SchemaError]:
        if item is not None:
            yield from self.schema.get_schema_errors(item, current_path)


def remove_optional(schema: SchemaABC[T]) -> SchemaABC[T]:
    return schema.schema if isinstance(schema, OptionalSchema) else schema
