from typing import Any

from dataclasses import dataclass
from marshy import ExternalType

from persisty.storage.field.write_transform.write_transform_abc import (
    WriteTransformABC,
    T,
)
from persisty.storage.field.write_transform.write_transform_mode import (
    WriteTransformMode,
)
from persisty.util.singleton_abc import SingletonABC
from persisty.util.undefined import UNDEFINED


@dataclass(frozen=True)
class DefaultValueTransform(WriteTransformABC):
    """
    A write transform which specifies a default value for a field.
    This works a little bit differently to datablasses with their
    field.default_factory - since all implementations deep copy the
    default_value, a default factory is not required."""

    default_value: ExternalType

    @property
    def mode(self) -> WriteTransformMode:
        return WriteTransformMode.OPTIONAL_FOR_CREATE

    def transform(self, specified_value: T, is_update: bool = False) -> ExternalType:
        if not is_update and specified_value is UNDEFINED:
            return self.default_value
        return specified_value


NULLABLE = DefaultValueTransform(None)
