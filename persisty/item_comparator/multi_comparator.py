from dataclasses import dataclass
from typing import Iterator, Any

from persisty.item_comparator.item_comparator_abc import ItemComparatorABC, T


@dataclass(frozen=True)
class MultiComparator(ItemComparatorABC[T]):
    comparators: Iterator[ItemComparatorABC[T]]

    def key(self, item: T) -> Any:
        key = [c.key(item) for c in self.comparators]
        return key
