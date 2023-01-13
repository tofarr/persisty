from uuid import uuid4

from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.util.undefined import UNDEFINED


class UuidGenerator(AttrValueGeneratorABC):
    def transform(self, value):
        if value is UNDEFINED:
            return uuid4()
        else:
            return value
