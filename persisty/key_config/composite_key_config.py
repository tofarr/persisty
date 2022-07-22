from dataclasses import dataclass
from typing import Optional, Tuple

from persisty.key_config.field_key_config import FieldKeyConfig
from persisty.key_config.obj_key_config_abc import ObjKeyConfigABC
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.util import to_base64, from_base64


@dataclass(frozen=True)
class CompositeKeyConfig(KeyConfigABC, ObjKeyConfigABC):
    fields: Tuple[FieldKeyConfig, ...]

    def get_key(self, item) -> str:
        keys = [f.get_key(item) for f in self.fields]
        key = to_base64(keys)
        return key

    def set_key(self, key: Optional[str], item):
        keys = from_base64(key)
        for field, key in zip(self.fields, keys):
            field.set_key(key, item)

    def is_required_field(self, field_name: str) -> bool:
        for field in self.fields:
            if field.is_required_field(field_name):
                return True
        return False


FIELD_KEY_CONFIG = FieldKeyConfig()
