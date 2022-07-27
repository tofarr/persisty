from dataclasses import dataclass
from typing import List, Dict, Iterable, Optional

from boto3.dynamodb.conditions import Key, And
from marshy.types import ExternalItemType

from persisty.impl.dynamodb.dynamodb_key_config import DynamodbKeyConfig
from persisty.key_config.field_key_config import FieldKeyConfig
from persisty.obj_storage.attr import Attr
from persisty.field.field import Field


@dataclass(frozen=True)
class DynamodbIndex:
    pk: str
    sk: Optional[str] = None

    def to_schema(self):
        schema = [dict(AttributeName=self.pk, KeyType="HASH")]
        if self.sk:
            schema.append(dict(AttributeName=self.sk, KeyType="RANGE"))
        return schema

    def to_condition_expression(self, item: ExternalItemType):
        condition_expression = Key(self.pk).eq(item[self.pk])
        if self.sk and item.get(self.sk) is not None:
            sk = Key(self.sk).eq(item[self.sk])
            condition_expression = And(condition_expression, sk)
        return condition_expression

    def to_dict(self, item: ExternalItemType):
        d = {self.pk: item[self.pk]}
        if self.sk:
            d[self.sk] = item[self.sk]
        return d

    def key_config_from_attrs(self, attrs: Iterable[Attr]):
        pk_attr = next(a for a in attrs if a.name == self.pk)
        pk = FieldKeyConfig(self.pk, pk_attr.field_type)
        if not self.sk:
            return pk
        sk_attr = next(a for a in attrs if a.name == self.sk)
        sk = FieldKeyConfig(self.sk, sk_attr.field_type)
        return DynamodbKeyConfig(pk, sk)

    def key_config_from_fields(self, fields: Iterable[Field]):
        pk_field = next(f for f in fields if f.name == self.pk)
        pk = FieldKeyConfig(self.pk, pk_field.type)
        if not self.sk:
            return pk
        sk_attr = next(f for f in fields if f.name == self.sk)
        sk = FieldKeyConfig(self.sk, sk_attr.type)
        return DynamodbKeyConfig(pk, sk)

    def _to_key_config(self, configs: List[FieldKeyConfig]):
        pk_field = next(f for f in configs if f.field_name == self.pk)
        if not self.sk:
            return pk_field
        sk_field = next(f for f in configs if f.field_name == self.sk)
        return DynamodbKeyConfig(pk_field, sk_field)


def from_schema(schema: List[Dict]):
    assert len(schema) == 1 or len(schema) == 2
    index = DynamodbIndex(
        pk=next(a["AttributeName"] for a in schema if a["KeyType"] == "HASH"),
        sk=next((a["AttributeName"] for a in schema if a["KeyType"] == "RANGE"), None),
    )
    return index


ID_INDEX = DynamodbIndex("id")
