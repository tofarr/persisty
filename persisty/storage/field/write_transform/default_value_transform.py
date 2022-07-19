from typing import Any

from dataclasses import dataclass
from marshy import ExternalType

from persisty.storage.field.write_transform.write_transform_abc import WriteTransformABC, T
from persisty.storage.field.write_transform.write_transform_mode import WriteTransformMode
from persisty.util.singleton_abc import SingletonABC
from persisty.util.undefined import UNDEFINED


@dataclass(frozen=True)
class DefaultValueTransform(WriteTransformABC):
    default_value: Any

    @property
    def mode(self) -> WriteTransformMode:
        return WriteTransformMode.OPTIONAL_FOR_CREATE

    def transform(self, specified_value: T, is_update: bool = False) -> ExternalType:
        if not is_update and specified_value is UNDEFINED:
            return self.default_value
        return specified_value


NULLABLE = DefaultValueTransform(None)
