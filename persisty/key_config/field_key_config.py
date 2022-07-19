from dataclasses import dataclass

from persisty.key_config.obj_key_config_abc import ObjKeyConfigABC
from persisty.storage.field.field_type import FieldType
from persisty.key_config.key_config_abc import KeyConfigABC


@dataclass(frozen=True)
class FieldKeyConfig(KeyConfigABC, ObjKeyConfigABC):
    id_attr_name: str = "id"
    field_type: FieldType = FieldType.STR

    def get_key(self, item) -> str:
        if hasattr(item, '__getitem__'):
            value = item.__getitem__(self.id_attr_name)
        else:
            value = getattr(item, self.id_attr_name)
        if value:
            value = str(value)
        return value

    def set_key(self, key: str, item):
        if key is not None:
            if self.field_type is FieldType.INT:
                key = int(key)
            elif self.field_type is FieldType.FLOAT:
                key = float(key)
        if hasattr(item, '__setitem__'):
            item.__setitem__(self.id_attr_name, key)
        else:
            setattr(item, self.id_attr_name, key)


FIELD_KEY_CONFIG = FieldKeyConfig()
