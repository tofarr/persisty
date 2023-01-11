from dataclasses import dataclass
from typing import Optional, Tuple

from aaaa.key_config.attr_key_config import AttrKeyConfig, T
from aaaa.key_config.key_config_abc import KeyConfigABC
from aaaa.util import to_base64, from_base64


@dataclass(frozen=True)
class CompositeKeyConfig(KeyConfigABC[T]):
    fields: Tuple[AttrKeyConfig, ...]

    def to_key_str(self, item: T) -> str:
        keys = [f.to_key_str(item) for f in self.fields]
        key = to_base64(keys)
        return key

    def from_key_str(self, key: Optional[str], output: T):
        if output is None:
            output = {}
        keys = from_base64(key)
        for field, key in zip(self.fields, keys):
            field.from_key_str(key, output)
        return output

    def is_required_attr(self, field_name: str) -> bool:
        for field in self.fields:
            if field.is_required_attr(field_name):
                return True
        return False
