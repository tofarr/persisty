from dataclasses import dataclass

from persisty.key_config.obj_key_config_abc import ObjKeyConfigABC
from persisty.storage.field.field_type import FieldType
from persisty.key_config.key_config_abc import KeyConfigABC


@dataclass(frozen=True)
class FieldKeyConfig(KeyConfigABC, ObjKeyConfigABC):
    field_name: str = "id"
    field_type: FieldType = FieldType.UUID

    def get_key(self, item) -> str:
        if hasattr(item, "__getitem__"):
            value = item.__getitem__(self.field_name)
        else:
            value = getattr(item, self.field_name)
        if value:
            value = str(value)
        return value

    def set_key(self, key: str, item):
        if key is not None:
            if self.field_type is FieldType.INT:
                key = int(key)
            elif self.field_type is FieldType.FLOAT:
                key = float(key)
        if hasattr(item, "__setitem__"):
            item.__setitem__(self.field_name, key)
        else:
            setattr(item, self.field_name, key)

    def is_required_field(self, field_name: str) -> bool:
        return self.field_name == field_name


FIELD_KEY_CONFIG = FieldKeyConfig()
