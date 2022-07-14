from dataclasses import dataclass

from persisty.search_filter.item_filter.item_filter_abc import ItemFilterABC, T


@dataclass(frozen=True)
class Not(ItemFilterABC[T]):
    item_filter: ItemFilterABC[T]

    def __new__(cls, item_filter: ItemFilterABC[T]):
        """ Strip out direct nested Not """
        if isinstance(item_filter, Not):
            return item_filter.item_filter
        return super(Not, cls).__new__(cls)

    def match(self, value: T) -> bool:
        return not self.item_filter.match(value)
