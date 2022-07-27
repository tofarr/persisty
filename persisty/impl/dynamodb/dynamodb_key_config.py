from dataclasses import dataclass
from typing import Optional, Any

from marshy.types import ExternalItemType

from persisty.key_config.field_key_config import FieldKeyConfig
from persisty.field.field_type import FieldType
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.util.encrypt_at_rest import decrypt, encrypt


@dataclass(frozen=True)
class DynamodbKeyConfig(KeyConfigABC):
    pk_field: FieldKeyConfig
    sk_field: Optional[FieldKeyConfig] = None

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
            if self.pk_field.field_type == FieldType.INT:
                key = int(key)
            elif self.pk_field.field_type == FieldType.FLOAT:
                key = float(key)
            self.pk_field.set_value_for(output, key)
        return output

    def is_required_field(self, field_name: str) -> bool:
        return (
            self.pk_field.field_name == field_name
            or self.sk_field
            and self.sk_field.field_name == field_name
        )
