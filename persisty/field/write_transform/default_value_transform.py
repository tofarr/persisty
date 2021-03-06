from dataclasses import dataclass
from typing import Dict, Optional, Type

from marshy import ExternalType
from marshy.types import ExternalItemType
from schemey import SchemaContext, Schema
from schemey.factory.schema_factory_abc import SchemaFactoryABC

from persisty.field.write_transform.write_transform_abc import (
    WriteTransformABC,
    T,
)
from persisty.field.write_transform.write_transform_mode import (
    WriteTransformMode,
)
from persisty.util.undefined import UNDEFINED


@dataclass(frozen=True)
class DefaultValueTransform(WriteTransformABC):
    """
    A write transform which specifies a default value for a field.
    This works a little bit differently to datablasses with their
    field.default_factory - since all implementations deep copy the
    default_value, a default factory is not required."""

    default_value: ExternalType

    def get_create_mode(self) -> WriteTransformMode:
        return WriteTransformMode.OPTIONAL

    def get_update_mode(self) -> WriteTransformMode:
        return WriteTransformMode.OPTIONAL

    def transform(self, specified_value: T, is_update: bool = False) -> ExternalType:
        if not is_update and specified_value is UNDEFINED:
            return self.default_value
        return specified_value


NULLABLE = DefaultValueTransform(None)
