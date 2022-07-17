from dataclasses import dataclass

from marshy.types import ExternalItemType

from persisty.storage.dynamodb.dynamodb_index import DynamodbIndex
from persisty.storage.field.field import Field
from persisty.storage.field.field_type import FieldType
from persisty.storage.key_config.key_config_abc import KeyConfigABC
from persisty.util.encrypt_at_rest import decrypt, encrypt


@dataclass(frozen=True)
class DynamodbKeyConfig(KeyConfigABC):
    dynamo_index: DynamodbIndex
    pk_field: Field

    def get_key(self, item: ExternalItemType) -> str:
        if not self.dynamo_index.sk:
            key = item.get(self.pk_field.name)
            key = str(key)
            return key
        return encrypt({
            self.dynamo_index.pk: item[self.dynamo_index.pk],
            self.dynamo_index.sk: item[self.dynamo_index.sk]
        })

    def set_key(self, key: str, item: ExternalItemType):
        if not self.dynamo_index.sk:
            if self.pk_field.type == FieldType.INT:
                key = int(key)
            elif self.pk_field.type == FieldType.BOOL:
                key = float(key)
            return {self.dynamo_index.pk: key}
        return decrypt(key)
