from typing import Tuple, Any, Optional, Union, Type, get_origin, get_args

from dataclasses import dataclass

from persisty.item.field import Field
from persisty.search_filter.item_filter.item_filter_abc import ItemFilterABC, T
from persisty.search_filter.item_filter.item_filter_factory_abc import ItemFilterFactoryABC
from persisty.util.undefined import UNDEFINED


@dataclass
class QueryFilter(ItemFilterABC[T]):
    fields: Tuple[Field[str]]
    query: str

    def __post_init__(self):
        object.__setattr__(self, 'query', self.query.lower())

    def match(self, item: T) -> bool:
        for field in self.fields:
            field_value = field.__get__(item, item.__class__)
            if isinstance(field_value, str):
                if self.query in field_value.lower():
                    return True
        return False


class QueryFilterFactory(ItemFilterFactoryABC):

    def create(self, item_fields: Tuple[Field, ...], filter_name: str, filter_value: Any) -> Optional[ItemFilterABC]:
        if filter_name != 'query' or not filter_value:
            return None
        # noinspection PyTypeChecker
        str_fields: Tuple[Field[str]] = tuple(f for f in item_fields if is_str(f.type))
        if str_fields:
            return QueryFilter(str_fields, filter_value)


def is_str(type_: Type):
    if type_ == str:
        return True
    if get_origin(type_) == Union:
        a = [a for a in get_args(type_) if a not in (None, type(None), UNDEFINED)]
        return a == [str]
    return False
