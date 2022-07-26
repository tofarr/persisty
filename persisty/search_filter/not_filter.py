from dataclasses import dataclass
from typing import Tuple

from marshy import ExternalType

from persisty.storage.field.field import Field
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC


@dataclass(frozen=True)
class Not(SearchFilterABC):
    search_filter: SearchFilterABC

    def __new__(cls, search_filter: SearchFilterABC):
        """Strip out direct nested Not"""
        if isinstance(search_filter, Not):
            return search_filter.search_filter
        elif search_filter is EXCLUDE_ALL:
            return INCLUDE_ALL
        elif search_filter is INCLUDE_ALL:
            return EXCLUDE_ALL
        return super(Not, cls).__new__(cls)

    def validate_for_fields(self, fields: Tuple[Field, ...]):
        self.search_filter.validate_for_fields(fields)

    def match(self, value: ExternalType, fields: Tuple[Field, ...]) -> bool:
        return not self.search_filter.match(value, fields)
