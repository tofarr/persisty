from uuid import uuid4, UUID
from dataclasses import dataclass

from persisty.key_config.key_config_abc import KeyConfigABC, T


@dataclass(frozen=True)
class UuidKeyConfig(KeyConfigABC):
    id_attr_name: str = "id"

    def get_key(self, item: T) -> str:
        return getattr(item, self.id_attr_name)

    def generate_key(self) -> str:
        return str(uuid4())

    def set_key(self, key: str, item: T):
        setattr(item, self.id_attr_name, UUID(key))


UUID_KEY_CONFIG = UuidKeyConfig()
