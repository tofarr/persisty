from dataclasses import dataclass
from typing import List, Dict, Iterable, Optional

from boto3.dynamodb.conditions import Key, And as DynAnd
from marshy.types import ExternalItemType

from persisty.attr.attr import Attr
from persisty.impl.dynamodb.dynamodb_key_config import DynamodbKeyConfig
from persisty.key_config.attr_key_config import AttrKeyConfig


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
            condition_expression = DynAnd(condition_expression, sk)
        return condition_expression

    def to_dict(self, item: ExternalItemType):
        d = {self.pk: item[self.pk]}
        if self.sk:
            d[self.sk] = item[self.sk]
        return d

    def key_config_from_attrs(self, attrs: Iterable[Attr]):
        pk_attr = next(a for a in attrs if a.name == self.pk)
        pk = AttrKeyConfig(self.pk, pk_attr.attr_type)
        if not self.sk:
            return pk
        sk_attr = next(a for a in attrs if a.name == self.sk)
        sk = AttrKeyConfig(self.sk, sk_attr.attr_type)
        return DynamodbKeyConfig(pk, sk)

    def _to_key_config(self, configs: List[AttrKeyConfig]):
        pk_attr = next(a for a in configs if a.attr_name == self.pk)
        if not self.sk:
            return pk_attr
        sk_attr = next(a for a in configs if a.attr_name == self.sk)
        return DynamodbKeyConfig(pk_attr, sk_attr)


def from_schema(schema: List[Dict]):
    assert len(schema) == 1 or len(schema) == 2
    index = DynamodbIndex(
        pk=next(a["AttributeName"] for a in schema if a["KeyType"] == "HASH"),
        sk=next((a["AttributeName"] for a in schema if a["KeyType"] == "RANGE"), None),
    )
    return index


ID_INDEX = DynamodbIndex("id")
