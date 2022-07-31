from dataclasses import dataclass
from datetime import datetime

from persisty.field.write_transform.write_transform_abc import (
    WriteTransformABC,
    T,
)
from persisty.field.write_transform.write_transform_mode import (
    WriteTransformMode,
)
from persisty.util.undefined import UNDEFINED


@dataclass(frozen=True)
class TimestampGenerator(WriteTransformABC):

    on_update: bool = False
    allow_override: bool = False

    def get_create_mode(self) -> WriteTransformMode:
        if self.allow_override:
            return WriteTransformMode.OPTIONAL
        else:
            return WriteTransformMode.GENERATED

    def get_update_mode(self) -> WriteTransformMode:
        if not self.on_update:
            return WriteTransformMode.SPECIFIED
        if self.allow_override:
            return WriteTransformMode.OPTIONAL
        else:
            return WriteTransformMode.GENERATED

    def transform(self, specified_value: T, is_update: bool = False) -> T:
        if is_update and not self.on_update:
            return UNDEFINED
        else:
            return datetime.now().isoformat()


CREATED_AT_GENERATOR = TimestampGenerator()
UPDATED_AT_GENERATOR = TimestampGenerator(True)
