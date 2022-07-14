from datetime import datetime
from typing import Union

from attr import dataclass

from persisty.item.generator.generator_abc import GeneratorABC, T
from persisty.util.undefined import Undefined, UNDEFINED


@dataclass
class TimestampGenerator(GeneratorABC[datetime]):

    on_update: bool = False

    def generate_value(self, specified_value: Union[Undefined, datetime], is_update: bool = False) -> datetime:
        if is_update and not self.on_update:
            return UNDEFINED
        else:
            return datetime.now()


CREATED_AT_GENERATOR = TimestampGenerator()
UPDATED_AT_GENERATOR = TimestampGenerator(true)
