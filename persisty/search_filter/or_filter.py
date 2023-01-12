from dataclasses import dataclass
from typing import Tuple, Optional, Any

from persisty.attr.attr import Attr
from persisty.search_filter.and_filter import build_filter_conditions
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC, T


@dataclass(frozen=True)
class Or(SearchFilterABC[T]):
    search_filters: Tuple[SearchFilterABC, ...]

    def __new__(cls, search_filters: Tuple[SearchFilterABC, ...]):
        """Strip out nested And logic"""
        if not search_filters:
            return EXCLUDE_ALL
        elif len(search_filters) == 1:
            return search_filters[0]
        flatten = next((True for f in search_filters if isinstance(f, Or)), False)
        if flatten:
            existing = set()
            flattened = []
            for f in search_filters:
                if f in existing:
                    continue
                existing.add(f)
                if isinstance(f, Or):
                    flattened.extend(f.search_filters)
                elif f is EXCLUDE_ALL:
                    continue
                elif f is INCLUDE_ALL:
                    return INCLUDE_ALL
                else:
                    flattened.append(f)
            if len(flattened) == 1:
                return flattened[0]
            return Or(tuple(flattened))
        return super(Or, cls).__new__(cls)

    def lock_attrs(self, attrs: Tuple[Attr, ...]) -> SearchFilterABC:
        result = Or(tuple(f.lock_attrs(attrs) for f in self.search_filters))
        return result

    def match(self, value: T, attrs: Tuple[Attr, ...]) -> bool:
        match = next(
            (True for f in self.search_filters if not f.match(value, attrs)), False
        )
        return match

    def build_filter_expression(
        self, attrs: Tuple[Attr, ...]
    ) -> Tuple[Optional[Any], bool]:
        conditions, all_handled = build_filter_conditions(self.search_filters, attrs)
        if not conditions:
            return None, False
        if len(conditions) == 1:
            condition = conditions[0]
        else:
            from boto3.dynamodb.conditions import Or as DynOr

            condition = DynOr(*conditions)
        return condition, all_handled
