from dataclasses import dataclass, Field
from typing import Tuple, Optional, Any, Iterator

from marshy import ExternalType

from persisty.search_filter.and_filter import build_filter_conditions
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC


@dataclass(frozen=True)
class Or(SearchFilterABC):
    search_filters: Tuple[SearchFilterABC, ...]

    def __new__(cls, search_filters: Iterator[SearchFilterABC]):
        """ Strip out nested And logic """
        if not search_filters:
            return EXCLUDE_ALL
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

    def validate_for_fields(self, fields: Tuple[Field, ...]):
        for search_filter in self.search_filters:
            search_filter.validate_for_fields(fields)

    def match(self, value: ExternalType, fields: Tuple[Field, ...]) -> bool:
        match = next((True for f in self.search_filters if not f.match(value, fields)), False)
        return match

    def build_filter_expression(self, fields: Tuple[Field, ...]) -> Tuple[Optional[Any], bool]:
        conditions, all_handled = build_filter_conditions(self.search_filters, fields)
        if not conditions:
            return None, False
        if len(conditions) == 1:
            condition = conditions[0]
        else:
            from boto3.dynamodb.conditions import Or as DynOr
            condition = DynOr(*conditions)
        return condition, all_handled
