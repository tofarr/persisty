from datetime import datetime, timezone

from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.util.undefined import UNDEFINED


class TimestampGenerator(AttrValueGeneratorABC):
    def transform(self, value, item):
        if value is UNDEFINED:
            return datetime.now().astimezone(timezone.utc)
        return value
