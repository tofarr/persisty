from typing import Union
from uuid import uuid4, UUID

from persisty.item.generator.generator_abc import GeneratorABC, T
from persisty.util.singleton_abc import SingletonABC
from persisty.util.undefined import Undefined


class UuidGenerator(SingletonABC, GeneratorABC[UUID]):

    def generate_value(self, specified_value: Union[Undefined, UUID], is_update: bool = False) -> UUID:
        if not is_update and not specified_value:
            return uuid4()
        return specified_value


UUID_GENERATOR = UuidGenerator()
