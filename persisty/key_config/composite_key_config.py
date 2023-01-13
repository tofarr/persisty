from dataclasses import dataclass
from typing import Tuple, Iterator

from marshy.types import ExternalItemType

from persisty.key_config.attr_key_config import AttrKeyConfig, T
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.util import to_base64, from_base64


@dataclass(frozen=True)
class CompositeKeyConfig(KeyConfigABC[T]):
    attrs: Tuple[AttrKeyConfig, ...]

    def to_key_str(self, item: T) -> str:
        keys = [a.to_key_str(item) for a in self.attrs]
        key = to_base64(keys)
        return key

    def from_key_str(self, key: str, target: T):
        result = from_base64(key)
        for attr, value in zip(self.attrs, result):
            setattr(target, attr.attr_name, value)

    def to_key_dict(self, key: str) -> ExternalItemType:
        key_parts = from_base64(key)
        result = {
            attr.attr_name: attr.to_key_attr(value)
            for attr, value in zip(self.attrs, key_parts)
        }
        return result

    def get_key_attrs(self) -> Iterator[str]:
        key_attrs = getattr(self, "_key_attrs", None)
        if not key_attrs:
            key_attrs = frozenset(a.attr_name for a in self.attrs)
            object.__setattr__(self, "_key_attrs", key_attrs)
        return key_attrs
