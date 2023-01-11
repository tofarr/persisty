from uuid import uuid4

from aaaa.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from aaaa.util.undefined import UNDEFINED


class UuidGenerator(AttrValueGeneratorABC):

    def transform(self, value):
        if value is UNDEFINED:
            return uuid4()
        else:
            return value

