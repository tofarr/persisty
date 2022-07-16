from typing import Union
from uuid import uuid4, UUID

from dataclasses import dataclass

from persisty.item.generator.generator_abc import GeneratorABC
from persisty.item.generator.generator_mode import GeneratorMode
from persisty.util.undefined import Undefined

@dataclass
class UuidGenerator(GeneratorABC[UUID]):
    """
    UUID Generator. Note: There could be a security concern with making this optional for create operations - for
    filtered storage, it could open a way for attackers to check if an id exists. (By trying to create it)
    """
    always: bool = True

    def generator_mode(self) -> GeneratorMode:
        return GeneratorMode.ALWAYS_FOR_CREATE if self.always else GeneratorMode.OPTIONAL_FOR_CREATE

    def generate_value(self, specified_value: Union[Undefined, UUID], is_update: bool = False) -> UUID:
        if is_update:
            return specified_value
        if self.always or not specified_value:
            return uuid4()
        return specified_value


UUID_OPTIONAL_ON_CREATE = UuidGenerator(False)
UUID_ALWAYS_ON_CREATE = UuidGenerator(True)
