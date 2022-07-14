from dataclasses import dataclass
from typing import Tuple

from persisty.search_filter.item_filter.always_true import ALWAYS_TRUE
from persisty.search_filter.item_filter.item_filter_abc import ItemFilterABC, T


@dataclass(frozen=True)
class And(ItemFilterABC[T]):
    item_filters: Tuple[ItemFilterABC[T], ...]

    def __new__(cls, item_filters: Tuple[ItemFilterABC[T], ...]):
        """ Strip out nested And logic """
        if not item_filters:
            return ALWAYS_TRUE
        flatten = next((True for f in item_filters if isinstance(f, And)), False)
        if flatten:
            flattened = []
            for f in item_filters:
                if isinstance(f, And):
                    flattened.extend(f.item_filters)
                else:
                    flattened.append(f)
            if len(flattened) == 1:
                return flattened[0]
            return And(tuple(flattened))
        return super(And, cls).__new__(cls)

    def match(self, value: T) -> bool:
        match = next((False for f in self.item_filters if not f.match(value)), True)
        return match
