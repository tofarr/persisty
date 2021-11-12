from dataclasses import dataclass

from persisty2.item_filter.item_filter_abc import ItemFilterABC, T


@dataclass(frozen=True)
class NotFilter(ItemFilterABC[T]):
    filter: ItemFilterABC[T]

    def match(self, item: T) -> bool:
        return not self.filter.match(item)
