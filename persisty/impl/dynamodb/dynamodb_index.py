from dataclasses import dataclass
from typing import List, Dict

from boto3.dynamodb.conditions import Key, And
from marshy.types import ExternalItemType


@dataclass(frozen=True)
class DynamodbIndex:
    pk: str
    sk: str

    def to_schema(self):
        schema = [dict(AttributeName=self.pk, KeyType='HASH')]
        if self.sk:
            schema.append(dict(AttributeName=self.sk, KeyType='RANGE'))
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


def from_schema(schema: List[Dict]):
    assert len(schema) == 1 or len(schema) == 2
    index = DynamodbIndex(
        pk=next(a['AttributeName'] for a in schema if a['KeyType'] == 'HASH'),
        sk=next((a['AttributeName'] for a in schema if a['KeyType'] == 'RANGE'), None)
    )
    return index
