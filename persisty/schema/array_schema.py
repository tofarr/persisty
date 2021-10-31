from dataclasses import dataclass
from typing import Optional, List, Iterator

from persisty.schema.json_schema_abc import JsonSchemaABC
from persisty.schema.schema_error import SchemaError
from persisty.schema.schema_abc import T


@dataclass(frozen=True)
class ArraySchema(JsonSchemaABC[List[T]]):
    item_schema: Optional[JsonSchemaABC[T]] = None
    min_items: int = 0
    max_items: Optional[int] = None
    uniqueness: bool = False

    def get_schema_errors(self,
                          items: List[T],
                          current_path: Optional[List[str]] = None
                          ) -> Iterator[SchemaError]:
        if current_path is None:
            current_path = []
        if self.item_schema is not None:
            for index, item in enumerate(items):
                current_path.append(str(index))
                yield from self.item_schema.get_schema_errors(item, current_path)
                current_path.pop()
        if self.min_items is not None and len(items) < self.min_items:
            yield SchemaError(current_path, 'min_length', items)
        if self.max_items is not None and len(items) > self.max_items:
            yield SchemaError(current_path, 'max_length', items)
