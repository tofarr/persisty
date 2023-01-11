from dataclasses import dataclass
from typing import Any

from aaaa.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from aaaa.util.undefined import UNDEFINED


@dataclass
class DefaultValueGenerator(AttrValueGeneratorABC):
    default_value: Any

    def transform(self, value):
        if value is UNDEFINED:
            return self.default_value

