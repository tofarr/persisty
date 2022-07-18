from dataclasses import dataclass

from marshy.types import ExternalItemType

from persisty.storage.field.field_type import FieldType
from persisty.key_config.key_config_abc import KeyConfigABC


@dataclass(frozen=True)
class FieldKeyConfig(KeyConfigABC[ExternalItemType]):
    id_attr_name: str = "id"
    field_type: FieldType = FieldType.STR

    def get_key(self, item: T) -> str:
        value = getattr(item, self.id_attr_name)
        if value:
            value = str(value)
        return value

    def set_key(self, key: str, item: ExternalItemType):
        if key is not None:
            if self.field_type is FieldType.INT:
                key = int(key)
            elif self.field_type is FieldType.FLOAT:
                key = float(key)
        item[self.id_attr_name] = key


ATTR_KEY_CONFIG = AttrKeyConfig()
