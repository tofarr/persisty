from dataclasses import dataclass
from typing import Union, Iterable, Sized, Optional

from marshy.types import ExternalItemType

from lambsync.persistence.dynamo.dynamo_index import DynamoIndex


@dataclass(frozen=True)
class DynamoTable:
    name: str
    primary_index: DynamoIndex
    global_secondary_indexes: Union[Iterable[DynamoIndex], Sized] = tuple()
    query_attrs: Union[Iterable[str], Sized] = tuple()

    @staticmethod
    def choose_index(self, search_filter: ExternalItemType) -> Optional[DynamoIndex]:
        """
        When conducting a search, we want to be able to automatically figure out which index to use.
        In order to do this, we assign each a score for the search and pick the best one
        """
        best_score = self.primary_index.get_score_for_filter(search_filter)
        best_index = self.primary_index if best_score else None
        for index in self.global_secondary_indexes:
            score = index.get_score_for_filter(search_filter)
            if score > best_score:
                best_index = index
                best_score = score
        return best_index
