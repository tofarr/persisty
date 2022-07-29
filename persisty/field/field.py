from dataclasses import dataclass, field
from typing import Optional, Tuple, Iterable, Any

from marshy.types import ExternalItemType
from schemey import Schema

from persisty.field.field_filter import FieldFilterOp
from persisty.field.field_type import FieldType
from persisty.field.write_transform.write_transform_abc import WriteTransformABC
from persisty.util import UNDEFINED


@dataclass(frozen=True)
class Field:
    name: str
    type: FieldType
    schema: Schema = field(
        metadata=dict(schemey=Schema({}, Schema))
    )  # We arent validating the schema on fields for now
    is_readable: bool = True
    is_creatable: bool = True
    is_updatable: bool = True
    write_transform: Optional[WriteTransformABC] = None
    permitted_filter_ops: Tuple[FieldFilterOp, ...] = (
        FieldFilterOp.eq,
        FieldFilterOp.ne,
    )
    is_sortable: bool = True  # Note: dynamodb is not generally sortable!
    description: Optional[str] = None
    is_indexed: bool = False
    is_nullable: bool = True

    def get_value_for(self, item: Any):
        if hasattr(item, "__getitem__"):
            return item.get(self.name, UNDEFINED)
        return getattr(item, self.name)

    def set_value_for(self, value: Any, item: Optional[Any]) -> Any:
        if item is None:
            item = {}
        if hasattr(item, "__setitem__"):
            item.set(self.name, value)
        else:
            setattr(item, self.name, value)
        return item


def load_field_values(
    fields: Iterable[Field], item: ExternalItemType
) -> ExternalItemType:
    result = {}
    for field_ in fields:
        if not field_.is_readable:
            continue
        value = field_.get_value_for(item)
        if value is not UNDEFINED:
            result[field_.name] = value
    return result
