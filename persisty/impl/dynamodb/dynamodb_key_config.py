from dataclasses import dataclass
from typing import Optional, Any

from marshy.types import ExternalItemType

from persisty.storage.field.field import Field
from persisty.storage.field.field_type import FieldType
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.util.encrypt_at_rest import decrypt, encrypt


@dataclass(frozen=True)
class DynamodbKeyConfig(KeyConfigABC):
    pk_field: Field
    sk_field: Optional[Field] = None

    def to_key_str(self, item: ExternalItemType) -> str:
        if not self.sk_field:
            key = self.pk_field.get_value_for(item)
            key = str(key)
            return key
        return encrypt([self.pk_field.get_value_for(item), self.sk_field.get_value_for(item)])

    def from_key_str(self, key: Optional[str], output: Optional[Any] = None) -> Any:
        if output is None:
            output = {}
        if self.sk_field:
            key = decrypt(key)
            self.pk_field.set_value_for(key[0], output)
            self.sk_field.set_value_for(key[1], output)
        else:
            if self.pk_field.type == FieldType.INT:
                key = int(key)
            elif self.pk_field.type == FieldType.FLOAT:
                key = float(key)
            self.pk_field.set_value_for(key, output)
        return output

    def is_required_field(self, field_name: str) -> bool:
        return self.pk_field.name == field_name or self.sk_field and self.sk_field.name == field_name
