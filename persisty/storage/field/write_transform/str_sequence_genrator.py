from uuid import uuid4

from dataclasses import dataclass

from persisty.storage.field.write_transform.write_transform_abc import (
    WriteTransformABC,
    T,
)
from persisty.storage.field.write_transform.write_transform_mode import (
    WriteTransformMode,
)


@dataclass
class StrSequenceGenerator(WriteTransformABC):
    """
    Sequence Id Generator. Note: There could be a security concern with making this optional for create operations - for
    filtered storage, it could open a way for attackers to check if an id exists. (By trying to create it)
    Typically sql based keys will generate this value in the database rather than relying on a client.
    """
    format = "{value}"
    always: bool = True
    value: int = 1
    step: int = 1

    def mode(self) -> WriteTransformMode:
        return (
            WriteTransformMode.ALWAYS_FOR_CREATE
            if self.always
            else WriteTransformMode.OPTIONAL_FOR_CREATE
        )

    def transform(self, specified_value: T, is_update: bool = False) -> T:
        if is_update:
            return specified_value
        if self.always or not specified_value:
            value = self.format.format(value=self.value)
            self.value += self.step
            return value
        return specified_value
