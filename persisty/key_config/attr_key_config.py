from uuid import uuid4, UUID
from dataclasses import dataclass

from persisty.key_config.key_config_abc import KeyConfigABC, T


@dataclass(frozen=True)
class AttrKeyConfig(KeyConfigABC):
    id_attr_name: str = "id"

    def get_key(self, item: T) -> str:
        value = getattr(item, self.id_attr_name)
        if value:
            value = str(value)
        return value


ATTR_KEY_CONFIG = AttrKeyConfig()
