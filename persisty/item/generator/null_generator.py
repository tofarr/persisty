from typing import Union

from persisty.item.generator.generator_abc import GeneratorABC, T
from persisty.util.singleton_abc import SingletonABC
from persisty.util.undefined import Undefined, UNDEFINED


class NullGenerator(SingletonABC, GeneratorABC):

    def generate_value(self, specified_value: Union[Undefined, T], is_update: bool = False) -> Union[Undefined, T]:
        if not is_update and specified_value is UNDEFINED:
            return None
        return specified_value


NULL_GENERATOR = NullGenerator()
