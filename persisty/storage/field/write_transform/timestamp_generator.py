from dataclasses import dataclass
from datetime import datetime

from persisty.storage.field.write_transform.write_transform_abc import (
    WriteTransformABC,
    T,
)
from persisty.storage.field.write_transform.write_transform_mode import (
    WriteTransformMode,
)
from persisty.util.undefined import UNDEFINED


@dataclass(frozen=True)
class TimestampGenerator(WriteTransformABC):

    on_update: bool = False

    def mode(self) -> WriteTransformMode:
        return (
            WriteTransformMode.ALWAYS_FOR_WRITE
            if self.on_update
            else WriteTransformMode.ALWAYS_FOR_UPDATE
        )

    def transform(self, specified_value: T, is_update: bool = False) -> T:
        if is_update and not self.on_update:
            return UNDEFINED
        else:
            return str(datetime.now())


CREATED_AT_GENERATOR = TimestampGenerator()
UPDATED_AT_GENERATOR = TimestampGenerator(True)
