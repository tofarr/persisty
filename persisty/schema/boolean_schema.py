from dataclasses import dataclass
from typing import Optional, List, Iterator

from persisty.schema.json_schema_abc import JsonSchemaABC
from persisty.schema.schema_error import SchemaError


@dataclass(frozen=True)
class BooleanSchema(JsonSchemaABC[bool]):

    def get_schema_errors(self, item: bool, current_path: Optional[List[str]] = None) -> Iterator[SchemaError]:
        if not isinstance(item, bool):
            yield SchemaError(current_path or [], 'type', item)
