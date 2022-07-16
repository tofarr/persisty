from dataclasses import dataclass

from marshy.types import ExternalItemType

from persisty.storage.key_config.key_config_abc import KeyConfigABC


@dataclass(frozen=True)
class AttrKeyConfig(KeyConfigABC):
    id_attr_name: str = "id"

    def get_key(self, item: T) -> str:
        value = getattr(item, self.id_attr_name)
        if value:
            value = str(value)
        return value

    def set_key(self, key: str, item: ExternalItemType):
        item[self.id_attr_name] = key


ATTR_KEY_CONFIG = AttrKeyConfig()
