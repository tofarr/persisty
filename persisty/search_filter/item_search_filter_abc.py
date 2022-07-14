from abc import ABC
from typing import Any, Tuple

from persisty.item.field import fields_for_type, Field
from persisty.search_filter.item_filter.and_filter import And
from persisty.search_filter.item_filter.item_filter_abc import ItemFilterABC
from persisty.search_filter.item_filter.query_filter import QueryFilterFactory
from persisty.search_filter.search_filter_abc import SearchFilterABC, T
from persisty.util.undefined import UNDEFINED

from persisty.search_filter.item_filter.eq import EQ_FACTORY
from persisty.search_filter.item_filter.gt import GT_FACTORY
from persisty.search_filter.item_filter.gte import GTE_FACTORY
from persisty.search_filter.item_filter.lt import LT_FACTORY
from persisty.search_filter.item_filter.lte import LTE_FACTORY

FACTORIES = [
    QueryFilterFactory(),
    EQ_FACTORY,
    GT_FACTORY,
    GTE_FACTORY,
    LT_FACTORY,
    LTE_FACTORY,
]


def create_item_filter(item_fields: Tuple[Field, ...], filter_name: str, filter_value: Any) -> ItemFilterABC:
    for factory in FACTORIES:
        item_filter = factory.create(item_fields, filter_name, filter_value)
        if item_filter:
            return item_filter
    raise ValueError(f'item_filter_create_failed:{filter_name}:{filter_value}')


class ItemSearchFilterABC(ABC, SearchFilterABC[T]):
    """ A search filter which is an item, and uses reflection to build a matcher. """

    def create_item_filter(self, fields: Tuple[Field, ...], type_: T) -> ItemFilterABC:
        item_filters = []
        for filter_field in fields_for_type(self.__class__):
            filter_value = filter_field.__get__(self, self.__class__)
            if filter_value is not UNDEFINED:
                item_filter = create_item_filter(fields, filter_field.name, filter_value)
                item_filters.append(item_filter)
        return And(tuple(item_filters))

    def match(self, fields: Tuple[Field, ...], item: T) -> bool:
        item_filter = self.create_item_filter(fields, item.__class__)
        if not item_filter.match(item):
            return False
        return True
