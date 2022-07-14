from dataclasses import dataclass

from persisty.item.field import Field
from persisty.search_filter.item_filter.item_filter_abc import ItemFilterABC, T
from persisty.search_filter.item_filter.suffix_filter_factory import SuffixFactory


@dataclass(frozen=True)
class Gt(ItemFilterABC[T]):
    field: Field[T]
    value: T

    def match(self, item: T) -> bool:
        value = self.field.__get__(item, item.__class__)
        return value > self.value


GT_FACTORY = SuffixFactory('__gt', Gt)
