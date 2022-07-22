from dataclasses import dataclass
from typing import Optional, Any

from marshy.types import ExternalItemType

from persisty.key_config.obj_key_config_abc import ObjKeyConfigABC
from persisty.storage.field.field_type import FieldType
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.util import UNDEFINED


@dataclass(frozen=True)
class FieldKeyConfig(KeyConfigABC, ObjKeyConfigABC):
    field_name: str = "id"
    field_type: FieldType = FieldType.UUID

    def to_key_str(self, item) -> str:
        if hasattr(item, "__getitem__"):
            value = item.get(self.field_name)
        else:
            value = getattr(item, self.field_name)
        if value:
            value = str(value)
        return value

    def from_key_str(self, key: Optional[str], output: Optional[Any] = None) -> Any:
        if output is None:
            output = {}
        if key is not None:
            if key is None or key is UNDEFINED:
                key = UNDEFINED
            if self.field_type is FieldType.INT:
                key = int(key)
            elif self.field_type is FieldType.FLOAT:
                key = float(key)
        if hasattr(output, "__setitem__"):
            output.__setitem__(self.field_name, key)
        else:
            setattr(output, self.field_name, key)
        return output

    def is_required_field(self, field_name: str) -> bool:
        return self.field_name == field_name


FIELD_KEY_CONFIG = FieldKeyConfig()
