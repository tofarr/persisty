from dataclasses import dataclass
from typing import Optional, Any, Iterator

from marshy.types import ExternalItemType

from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.attr.attr_type import AttrType
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.util.encrypt_at_rest import decrypt, encrypt


@dataclass(frozen=True)
class DynamodbKeyConfig(KeyConfigABC):
    pk_attr: AttrKeyConfig
    sk_attr: Optional[AttrKeyConfig] = None

    def to_key_str(self, item: ExternalItemType) -> str:
        if not self.sk_attr:
            key = self.pk_attr.get_value_for(item)
            key = str(key)
            return key
        return encrypt(
            [self.pk_attr.get_value_for(item), self.sk_attr.get_value_for(item)]
        )

    def from_key_str(self, key: Optional[str], output: Optional[Any] = None) -> Any:
        if output is None:
            output = {}
        if self.sk_attr:
            key = decrypt(key)
            self.pk_attr.set_value_for(output, key[0])
            self.sk_attr.set_value_for(output, key[1])
        else:
            if self.pk_attr.attr_type == AttrType.INT:
                key = int(key)
            elif self.pk_attr.attr_type == AttrType.FLOAT:
                key = float(key)
            self.pk_attr.set_value_for(output, key)
        return output

    def get_required_attrs(self) -> Iterator[str]:
        yield self.pk_attr
        yield self.sk_attr
