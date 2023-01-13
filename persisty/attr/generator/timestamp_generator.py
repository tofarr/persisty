from datetime import datetime, timezone

from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.util.undefined import UNDEFINED


class TimestampGenerator(AttrValueGeneratorABC):
    def transform(self, value):
        if value is UNDEFINED:
            return datetime.now().astimezone(timezone.utc)
        else:
            return value
