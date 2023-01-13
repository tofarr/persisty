from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Iterator
from uuid import UUID

import marshy
from marshy.types import ExternalItemType, ExternalType

from persisty.attr.attr_type import AttrType
from persisty.errors import PersistyError
from persisty.key_config.key_config_abc import KeyConfigABC, T
from persisty.util import UNDEFINED


@dataclass(frozen=True)
class AttrKeyConfig(KeyConfigABC[T]):
    attr_name: str = "id"
    attr_type: AttrType = AttrType.UUID

    def to_key_str(self, item: T) -> str:
        value = getattr(item, self.attr_name)
        if not isinstance(value, str):
            if value in (None, UNDEFINED):
                raise PersistyError("invalid_key")
            value = str(value)
        return value

    def from_key_str(self, key: str, target: T):
        setattr(target, self.attr_name, self.to_key_attr(key))

    def to_key_dict(self, key: str) -> ExternalItemType:
        result = {self.attr_name: self.to_key_attr(key)}
        return result

    def to_key_attr(self, key: str) -> ExternalType:
        if key not in (None, UNDEFINED):
            if self.attr_type is AttrType.INT:
                return int(key)
            elif self.attr_type is AttrType.FLOAT:
                return float(key)
            elif self.attr_type in (AttrType.UUID, AttrType.STR):
                return str(key)
            elif self.attr_name is AttrType.DATETIME:
                if isinstance(key, datetime):
                    return marshy.dump(key)
                elif isinstance(key, str):
                    return key
            elif self.attr_name is AttrType.BOOL:
                return bool(key)
        raise PersistyError("invalid_type")

    def get_key_attrs(self) -> Iterator[str]:
        key_attrs = getattr(self, "_key_attrs", None)
        if not key_attrs:
            key_attrs = frozenset((self.attr_name,))
            object.__setattr__(self, "_key_attrs", key_attrs)
        return key_attrs


ATTR_KEY_CONFIG = AttrKeyConfig()
