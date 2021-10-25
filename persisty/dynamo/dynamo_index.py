from dataclasses import dataclass
from typing import Optional

from marshy.types import ExternalItemType

from lambsync.persistence.dynamo.dynamo_attr_type import DynamoAttrType
from lambsync.persistence.util import to_base64, from_base64


@dataclass(frozen=True)
class DynamoIndex:
    pk: str
    sk: Optional[str] = None
    name: Optional[str] = None
    pk_type = DynamoAttrType.STR
    sk_type = DynamoAttrType.STR

    def isolate_key(self, item: ExternalItemType) -> ExternalItemType:
        key = {self.pk: item[self.pk]}
        if self.sk and self.sk in item:
            key[self.sk] = item[self.sk]
        return key

    def key_to_str(self, item: ExternalItemType) -> str:
        if self.sk:
            return to_base64(self.isolate_key(item))
        else:
            return str(item[self.pk])

    def str_to_key(self, key_str: str) -> ExternalItemType:
        if self.sk:
            return self.isolate_key(from_base64(key_str))
        else:
            return {self.pk: key_str}

    def get_score_for_filter(self, search_filter: ExternalItemType) -> int:
        """
        We rank indexes against each other for particular filters so we can choose the best one to use for a search
        In order to do this, each index is given a score.
        """
        score = 0
        partition_key_filter = f'{self.pk}__in'
        if not len(search_filter.get(partition_key_filter) or []) == 1:
            partition_key_filter = f'{self.pk}__eq'
        if not search_filter.get(partition_key_filter):
            return score
        score += 1  # Having a partition key means this can be used
        if self.name == 'default':
            score += 1  # We favor the default index over GSIs.
        if not self.sk:
            return score  # If there is no sort key, this is all we have
        if search_filter.get(f'{self.sk}__eq') or len(search_filter.get(f'{self.sk}__in') or []) == 1:
            # We have definite values for partition key and sort key - the strongest possible combo!
            score += 5
            return score
        for op in ['gt', 'gte', 'lt', 'lte', 'begins_with']:
            if search_filter.get(f'{self.sk}__{op}'):
                score += 2
                return score
        return score
