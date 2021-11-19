from dataclasses import dataclass

from persisty.attr.attr_mode import AttrMode
from persisty.key_config.key_config_abc import KeyConfigABC, T


@dataclass(frozen=True)
class AttrKeyConfig(KeyConfigABC[T]):
    key_generation: AttrMode = AttrMode.OPTIONAL
    attr: str = 'id'

    def get_key(self, item: T) -> str:
        key = getattr(item, self.attr)
        return key

    def set_key(self, item: T, key: str):
        setattr(item, self.attr, key)
