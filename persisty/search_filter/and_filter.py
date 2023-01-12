from dataclasses import dataclass
from typing import Tuple, Optional, Any, List

from persisty.attr.attr import Attr
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC, T


@dataclass(frozen=True)
class And(SearchFilterABC[T]):
    search_filters: Tuple[SearchFilterABC, ...]

    def __new__(cls, search_filters: Tuple[SearchFilterABC, ...]):
        """Strip out nested And logic"""
        if not search_filters:
            return INCLUDE_ALL
        elif len(search_filters) == 1:
            return search_filters[0]
        flatten = next((True for f in search_filters if isinstance(f, And)), False)
        if flatten:
            existing = set()
            flattened = []
            for f in search_filters:
                if f in existing:
                    continue
                existing.add(f)
                if isinstance(f, And):
                    flattened.extend(f.search_filters)
                elif f is INCLUDE_ALL:
                    continue
                elif f is EXCLUDE_ALL:
                    return EXCLUDE_ALL
                else:
                    flattened.append(f)
            if len(flattened) == 1:
                return flattened[0]
            return And(tuple(flattened))
        return super(And, cls).__new__(cls)

    def lock_attrs(self, attrs: Tuple[Attr, ...]) -> SearchFilterABC:
        result = And(tuple(f.lock_attrs(attrs) for f in self.search_filters))
        return result

    def match(self, value: T, attrs: Tuple[Attr, ...]) -> bool:
        match = next(
            (False for f in self.search_filters if not f.match(value, attrs)), True
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
            from boto3.dynamodb.conditions import And as DynAnd

            condition = DynAnd(*conditions)
        return condition, all_handled


def build_filter_conditions(
    search_filters: Tuple[SearchFilterABC, ...], attrs: Tuple[Attr, ...]
) -> Tuple[List[Any], bool]:
    conditions = []
    all_handled = True
    for search_filter in search_filters:
        condition, handled = search_filter.build_filter_expression(attrs)
        all_handled = all_handled and handled
        if condition:
            conditions.append(condition)
    return conditions, all_handled
