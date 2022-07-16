from datetime import datetime
from typing import Union

from dataclasses import dataclass

from persisty.item.generator.generator_abc import GeneratorABC, T
from persisty.util.undefined import Undefined, UNDEFINED


@dataclass(frozen=True)
class DefaultValueGenerator(GeneratorABC[T]):
    default_value: T

    def generate_value(self, specified_value: Union[Undefined, datetime], is_update: bool = False) -> datetime:
        if not is_update and specified_value is UNDEFINED:
            return self.default_value
        else:
            return specified_value
