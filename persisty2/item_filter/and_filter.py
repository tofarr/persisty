from dataclasses import dataclass
from typing import Iterable

from persisty2.item_filter.item_filter_abc import ItemFilterABC, T


@dataclass(frozen=True)
class AndFilter(ItemFilterABC[T]):
    filters: Iterable[ItemFilterABC[T]]

    def match(self, item: T) -> bool:
        for f in self.filters:
            if not f.match(item):
                return False
        return True

    def __and__(self, other):
        filters = list(self.filters)
        if isinstance(other, AndFilter):
            filters.extend(other.filters)
        else:
            filters.append(other)
        return AndFilter(tuple(filters))
