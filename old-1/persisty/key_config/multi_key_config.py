from dataclasses import dataclass
from typing import Iterable

from persisty.attr.attr_mode import AttrMode
from persisty.key_config.key_config_abc import KeyConfigABC, T
from persisty.util import from_base64, to_base64


@dataclass(frozen=True)
class MultiKeyConfig(KeyConfigABC[T]):
    attrs: Iterable[str]
    key_generation: AttrMode = AttrMode.OPTIONAL

    def get_key(self, item: T) -> str:
        values = [getattr(item, attr) for attr in self.attrs]
        key = to_base64(values)
        return key

    def set_key(self, item: T, key: str):
        values = from_base64(key)
        value_iter = iter(values)
        for attr in self.attrs:
            value = next(value_iter)
            setattr(item, attr, value)
