from dataclasses import dataclass
from typing import Tuple

from marshy import ExternalType

from persisty.storage.field.field import Field
from persisty.storage.search_filter.search_filter_abc import SearchFilterABC


@dataclass(frozen=True)
class Not(SearchFilterABC):
    item_filter: SearchFilterABC

    def __new__(cls, item_filter: SearchFilterABC):
        """ Strip out direct nested Not """
        if isinstance(item_filter, Not):
            return item_filter.item_filter
        return super(Not, cls).__new__(cls)

    def validate_for_fields(self, fields: Tuple[Field, ...]):
        self.item_filter.validate_for_fields(fields)

    def match(self, value: ExternalType, fields: Tuple[Field, ...]) -> bool:
        return not self.item_filter.match(value, fields)
