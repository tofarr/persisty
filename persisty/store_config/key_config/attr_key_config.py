from dataclasses import dataclass

from persisty.store_config.key_config.key_config_abc import KeyConfigABC, T
from persisty.store_config.key_config.key_generation import KeyGeneration


@dataclass(frozen=True)
class AttrKeyConfig(KeyConfigABC[T]):
    key_generation: KeyGeneration.GENERATED
    attr: str = 'id'

    def get_key(self, item: T) -> str:
        key = getattr(item, self.attr)
        return key

    def set_key(self, item: T, key: str):
        setattr(item, self.attr, key)
