from dataclasses import dataclass
from typing import Iterable

from persisty2.item_filter.item_filter_abc import ItemFilterABC, T


@dataclass(frozen=True)
class OrFilter(ItemFilterABC[T]):
    filters: Iterable[ItemFilterABC[T]]

    def match(self, item: T) -> bool:
        for f in self.filters:
            if f.match(item):
                return True
        return False
