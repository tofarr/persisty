from dataclasses import dataclass
from typing import Optional, Union, Iterable, Sized, Iterator

from boto3.dynamodb.conditions import ConditionBase
from marshy.types import ExternalItemType

from lambsync.persistence import dynamo
from lambsync.persistence.dynamo.dynamo_table import DynamoTable


@dataclass(frozen=True)
class DynamoSearch:
    table: DynamoTable
    index_name: Optional[str] = None
    key: Optional[ConditionBase] = None
    filter_expression: Optional[ConditionBase] = None
    projected_attributes: Union[Iterable[str], Sized, None] = None

    def search(self, exclusive_start_key: Optional[ExternalItemType] = None) -> Iterator[ExternalItemType]:
        return dynamo.search(table_name=self.table.name,
                             key=self.key,
                             index_name=self.index_name,
                             filter_expression=self.filter_expression,
                             projected_attributes=self.projected_attributes,
                             exclusive_start_key=exclusive_start_key)

    def count(self) -> int:
        return dynamo.count(table_name=self.table.name,
                            key=self.key,
                            index_name=self.index_name,
                            filter_expression=self.filter_expression)
