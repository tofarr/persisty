from datetime import datetime

from aaaa.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from aaaa.util.undefined import UNDEFINED


class TimestampGenerator(AttrValueGeneratorABC):

    def transform(self, value):
        if value is UNDEFINED:
            return datetime.now()
        else:
            return value

