from dataclasses import dataclass, Field
from typing import Tuple

from marshy import ExternalType

from persisty.storage.search_filter.exclude_all import EXCLUDE_ALL
from persisty.storage.search_filter.include_all import INCLUDE_ALL
from persisty.storage.search_filter.search_filter_abc import SearchFilterABC


@dataclass(frozen=True)
class Or(SearchFilterABC):
    item_filters: Tuple[SearchFilterABC, ...]

    def __new__(cls, item_filters: Tuple[SearchFilterABC, ...]):
        """ Strip out nested And logic """
        if not item_filters:
            return EXCLUDE_ALL
        flatten = next((True for f in item_filters if isinstance(f, Or)), False)
        if flatten:
            existing = set()
            flattened = []
            for f in item_filters:
                if f in existing:
                    continue
                existing.add(f)
                if isinstance(f, Or):
                    flattened.extend(f.item_filters)
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
        for item_filter in self.item_filters:
            item_filter.validate_for_fields(fields)

    def match(self, value: ExternalType, fields: Tuple[Field, ...]) -> bool:
        match = next((True for f in self.item_filters if not f.match(value, fields)), False)
        return match
