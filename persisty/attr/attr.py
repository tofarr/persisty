from dataclasses import dataclass, Field, field
from typing import Optional, Tuple

from schemey import Schema, schema_from_type

from persisty.attr.attr_filter_op import AttrFilterOp, TYPE_FILTER_OPS
from persisty.attr.attr_type import AttrType, attr_type, ATTR_TYPE_MAP
from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC

from persisty.util.undefined import UNDEFINED


@dataclass
class Attr:
    name: str = UNDEFINED  # Can be populated by __set_name__
    attr_type: AttrType = UNDEFINED
    schema: Schema = UNDEFINED
    creatable: bool = True
    readable: bool = True
    updatable: bool = True
    sortable: bool = False
    create_generator: Optional[AttrValueGeneratorABC] = None
    update_generator: Optional[AttrValueGeneratorABC] = None
    permitted_filter_ops: Tuple[AttrFilterOp, ...] = UNDEFINED

    def __set_name__(self, owner, name):
        if self.name is UNDEFINED:
            self.name = name
        annotations = owner.__dict__.get("__annotations__")
        type_ = annotations.get(name)
        if self.attr_type is UNDEFINED:
            self.attr_type = attr_type(type_)
        if self.schema is UNDEFINED:
            self.schema = schema_from_type(type_)
        if self.permitted_filter_ops is UNDEFINED:
            self.permitted_filter_ops = (
                TYPE_FILTER_OPS.get(self.attr_type) or DEFAULT_PERMITTED_FILTER_OPS
            )

    def to_field(self) -> Field:
        result = field(
            default=UNDEFINED, metadata=dict(schemey=self.schema, persisty=self)
        )
        result.name = self.name
        result.type = self.schema.python_type
        return result

    def sanitize_type(self, value):
        """
        Sanitize the type of an attribute value - try and make sure the type is correct.
        """
        if value in (None, UNDEFINED):
            return value
        type_ = ATTR_TYPE_MAP[self.attr_type]
        if isinstance(value, type_):
            return value
        value = type_(value)
        return value


DEFAULT_PERMITTED_FILTER_OPS = (
    AttrFilterOp.eq,
    AttrFilterOp.exists,
    AttrFilterOp.ne,
    AttrFilterOp.not_exists,
)
