from dataclasses import dataclass
from typing import Tuple

from persisty.attr.attr import Attr
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC, T


@dataclass(frozen=True)
class Not(SearchFilterABC[T]):
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

    def lock_attrs(self, attrs: Tuple[Attr, ...]) -> SearchFilterABC:
        result = Not(self.search_filter.lock_attrs(attrs))
        return result

    def match(self, value: T, attrs: Tuple[Attr, ...]) -> bool:
        return not self.search_filter.match(value, attrs)
