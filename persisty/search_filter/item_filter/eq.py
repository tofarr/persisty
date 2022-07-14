from dataclasses import dataclass
from typing import Any, Optional, Tuple

from persisty.item.field import Field
from persisty.search_filter.item_filter.item_filter_abc import ItemFilterABC, T
from persisty.search_filter.item_filter.item_filter_factory_abc import ItemFilterFactoryABC
from persisty.search_filter.item_filter.suffix_filter_factory import SuffixFactory


@dataclass(frozen=True)
class Eq(ItemFilterABC[T]):
    field: Field[T]
    value: T

    def match(self, item: T) -> bool:
        value = self.field.__get__(item, item.__class__)
        return value == self.value


EQ_FACTORY = SuffixFactory('__eq', Eq)
