from dataclasses import dataclass
from typing import Optional, Iterator
from uuid import UUID

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
                raise PersistyError('invalid_key')
            value = str(value)
        return value

    def from_key_str(self, key: Optional[str], target: T):
        if key is not None:
            if key is None or key is UNDEFINED:
                key = UNDEFINED
            if self.attr_type is AttrType.INT:
                key = int(key)
            elif self.attr_type is AttrType.FLOAT:
                key = float(key)
            elif self.attr_type is AttrType.UUID:
                key = UUID(key)
        setattr(target, self.attr_name, key)

    def get_required_attrs(self) -> Iterator[str]:
        yield self.attr_name

    def get_value_for(self, item):
        if hasattr(item, "__getitem__"):
            return item.get(self.attr_name)
        else:
            return getattr(item, self.attr_name)

    def set_value_for(self, item, value):
        if item is None:
            item = {}
        if hasattr(item, "__setitem__"):
            item.__setitem__(self.attr_name, value)
        else:
            setattr(item, self.attr_name, value)
        return item


ATTR_KEY_CONFIG = AttrKeyConfig()
