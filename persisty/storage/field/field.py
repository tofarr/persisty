from dataclasses import dataclass
from typing import Optional, Tuple

from schemey.schema_abc import SchemaABC

from persisty.storage.field.field_filter import FieldFilterOp
from persisty.storage.field.field_type import FieldType
from persisty.storage.field.write_transform.write_transform_abc import WriteTransformABC


@dataclass(frozen=True)
class Field:
    name: str
    type: FieldType
    schema: SchemaABC
    is_readable: bool = True
    is_creatable: bool = True
    is_updatable: bool = True
    write_transform: Optional[WriteTransformABC] = None
    permitted_filter_ops: Tuple[FieldFilterOp, ...] = FieldFilterOp.eq, FieldFilterOp.ne
    description: Optional[str] = None
    indexed: bool = False
