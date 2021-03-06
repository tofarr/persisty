from uuid import uuid4

from dataclasses import dataclass

from persisty.field.write_transform.write_transform_abc import (
    WriteTransformABC,
    T,
)
from persisty.field.write_transform.write_transform_mode import (
    WriteTransformMode,
)


@dataclass
class UuidGenerator(WriteTransformABC):
    """
    UUID Generator. Note: There could be a security concern with making this optional for create operations - for
    filtered storage, it could open a way for attackers to check if an id exists. (By trying to create it)
    """

    always: bool = True

    def get_create_mode(self) -> WriteTransformMode:
        return (
            WriteTransformMode.GENERATED if self.always else WriteTransformMode.OPTIONAL
        )

    def get_update_mode(self) -> WriteTransformMode:
        return WriteTransformMode.SPECIFIED

    def transform(self, specified_value: T, is_update: bool = False) -> T:
        if is_update:
            return specified_value
        if self.always or not specified_value:
            return str(uuid4())
        return specified_value


UUID_OPTIONAL_ON_CREATE = UuidGenerator(False)
UUID_ALWAYS_ON_CREATE = UuidGenerator(True)
