from typing import Tuple, Iterator

from dataclasses import dataclass
from marshy.types import ExternalItemType

from persisty.search_order.search_order_field import SearchOrderField
from persisty.storage.field.field import Field


@dataclass(frozen=True)
class SearchOrder:
    orders: Tuple[SearchOrderField, ...]

    def validate_for_fields(self, fields: Tuple[Field, ...]):
        for order in self.orders:
            order.validate_for_fields(fields)

    def sort(self, items: Iterator[ExternalItemType]) -> Iterator[ExternalItemType]:
        for order in reversed(self.orders):
            items = order.sort(items)
        return items

    def lt(self, a: ExternalItemType, b: ExternalItemType) -> bool:
        for order in self.orders:
            if order.lt(a, b):
                return True
            elif not order.eq(a, b):
                return False

    def eq(self, a: ExternalItemType, b: ExternalItemType) -> bool:
        for order in self.orders:
            if not order.eq(a, b):
                return False
