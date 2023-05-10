from dataclasses import dataclass
from typing import Optional, Iterator

from marshy.types import ExternalItemType

from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.key_config.key_config_abc import KeyConfigABC, T
from persisty.util.encrypt_at_rest import decrypt, encrypt


@dataclass(frozen=True)
class DynamodbKeyConfig(KeyConfigABC):
    pk_attr: AttrKeyConfig
    sk_attr: Optional[AttrKeyConfig] = None

    def to_key_str(self, item: T) -> str:
        if not self.sk_attr:
            return self.pk_attr.to_key_str(item)
        return encrypt([self.pk_attr.to_key_str(item), self.sk_attr.to_key_str(item)])

    def from_key_str(self, key: Optional[str], target: T):
        if self.sk_attr:
            key = decrypt(key)
            self.pk_attr.from_key_str(key[0], target)
            self.sk_attr.from_key_str(key[1], target)
        else:
            self.pk_attr.from_key_str(key, target)

    def to_key_dict(self, key: Optional[str]) -> ExternalItemType:
        if self.sk_attr:
            key = decrypt(key)
            return {
                self.pk_attr.attr_name: self.pk_attr.to_key_attr(key[0]),
                self.sk_attr.attr_name: self.sk_attr.to_key_attr(key[1]),
            }
        return self.pk_attr.to_key_dict(key)

    def get_key_attrs(self) -> Iterator[str]:
        yield self.pk_attr
        yield self.sk_attr
