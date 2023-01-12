from typing import Tuple, Iterator, Generic

from dataclasses import dataclass

from persisty.attr.attr import Attr
from persisty.search_order.search_order_attr import SearchOrderAttr, T


@dataclass(frozen=True)
class SearchOrder(Generic[T]):
    orders: Tuple[SearchOrderAttr[T], ...]

    def validate_for_attrs(self, attrs: Tuple[Attr, ...]):
        for order in self.orders:
            order.validate_for_attrs(attrs)

    def sort(self, items: Iterator[T]) -> Iterator[T]:
        for order in reversed(self.orders):
            items = order.sort(items)
        return items

    def lt(self, a: T, b: T) -> bool:
        for order in self.orders:
            if order.lt(a, b):
                return True
            elif not order.eq(a, b):
                return False

    def eq(self, a: T, b: T) -> bool:
        for order in self.orders:
            if not order.eq(a, b):
                return False
