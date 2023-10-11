from dataclasses import dataclass
from typing import Any

from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.errors import PersistyError
from persisty.util import UNDEFINED


@dataclass
class FixedValueGenerator(AttrValueGeneratorABC):
    value: Any

    def transform(self, value, item):
        if value is not UNDEFINED and value != self.value:
            raise PersistyError("value_mismatch")
        return self.value
