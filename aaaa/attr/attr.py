from dataclasses import dataclass, Field, field
from typing import Optional, Tuple

from schemey import Schema

from aaaa.attr.attr_filter_op import AttrFilterOp
from aaaa.attr.attr_type import AttrType
from aaaa.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC

from aaaa.util.undefined import UNDEFINED


@dataclass
class Attr:
    name: str
    type: AttrType
    schema: Schema
    creatable: bool = True
    readable: bool = True
    updatable: bool = True
    searchable: bool = True
    sortable: bool = False
    create_transform: Optional[AttrValueGeneratorABC] = None
    update_transform: Optional[AttrValueGeneratorABC] = None
    permitted_filter_ops: Tuple[AttrFilterOp, ...] = (
        AttrFilterOp.eq,
        AttrFilterOp.exists,
        AttrFilterOp.ne,
        AttrFilterOp.not_exists,
    )

    def to_field(self) -> Field:
        result = field(default=UNDEFINED, metadata=dict(schemey=self.schema, persisty=self))
        result.name = self.name
        result.type = self.schema.python_type
        return result
