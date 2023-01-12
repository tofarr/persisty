from dataclasses import dataclass
from typing import Optional, Tuple, Iterator

import marshy

from persisty.attr.attr_type import AttrType
from persisty.key_config.attr_key_config import AttrKeyConfig, T
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.util import to_base64, from_base64, UNDEFINED


@dataclass(frozen=True)
class CompositeKeyConfig(KeyConfigABC[T]):
    attrs: Tuple[AttrKeyConfig, ...]

    def to_key_str(self, item: T) -> str:
        keys = [a.to_key_str(item) for a in self.attrs]
        key = to_base64(keys)
        return key

    def from_key_str(self, key: Optional[str], output: T):
        keys = from_base64(key)
        for attr, key in zip(self.attrs, keys):
            attr.from_key_str(key, output)

    def get_required_attrs(self) -> Iterator[str]:
        for attr in self.attrs:
            yield attr.attr_name
