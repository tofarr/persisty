from dataclasses import dataclass
from typing import Optional, Any

from marshy.types import ExternalItemType

from aaaa.key_config.attr_key_config import AttrKeyConfig
from aaaa.attr.attr_type import AttrType
from aaaa.key_config.key_config_abc import KeyConfigABC
from aaaa.util.encrypt_at_rest import decrypt, encrypt


@dataclass(frozen=True)
class DynamodbKeyConfig(KeyConfigABC):
    pk_field: AttrKeyConfig
    sk_field: Optional[AttrKeyConfig] = None

    def to_key_str(self, item: ExternalItemType) -> str:
        if not self.sk_field:
            key = self.pk_field.get_value_for(item)
            key = str(key)
            return key
        return encrypt(
            [self.pk_field.get_value_for(item), self.sk_field.get_value_for(item)]
        )

    def from_key_str(self, key: Optional[str], output: Optional[Any] = None) -> Any:
        if output is None:
            output = {}
        if self.sk_field:
            key = decrypt(key)
            self.pk_field.set_value_for(output, key[0])
            self.sk_field.set_value_for(output, key[1])
        else:
            if self.pk_field.attr_type == AttrType.INT:
                key = int(key)
            elif self.pk_field.attr_type == AttrType.FLOAT:
                key = float(key)
            self.pk_field.set_value_for(output, key)
        return output

    def is_required_attr(self, attr_name: str) -> bool:
        return (
            self.pk_field.attr_name == attr_name
            or self.sk_field
            and self.sk_field.attr_name == attr_name
        )
