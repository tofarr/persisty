from dataclasses import dataclass
from typing import Tuple

from persisty.item.field import Field
from persisty.search_filter.item_filter.item_filter_abc import ItemFilterABC, T


@dataclass(frozen=True)
class OneOf(ItemFilterABC[T]):
    field: Field[T]
    values: Tuple[T, ...]

    def match(self, item: T) -> bool:
        value = self.field.__get__(item, item.__class__)
        return value in self.values
