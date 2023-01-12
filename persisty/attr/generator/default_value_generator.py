from dataclasses import dataclass
from typing import Any

from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.util.undefined import UNDEFINED


@dataclass
class DefaultValueGenerator(AttrValueGeneratorABC):
    default_value: Any

    def transform(self, value):
        return self.default_value if value is UNDEFINED else value
