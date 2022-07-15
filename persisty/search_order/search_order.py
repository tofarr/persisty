from enum import Enum

from dataclasses import dataclass
from typing import Any, Tuple, Type

from marshy.factory.optional_marshaller_factory import get_optional_type

from persisty.item.field import Field
from persisty.search_order.search_order_abc import SearchOrderABC, T


@dataclass
class SearchOrder(SearchOrderABC):
    field_names: Tuple[Enum, ...]

    def key(self, item: T) -> Any:
        key = []
        for f in self.field_names:
            value = getattr(item, f.value)
            if value:
                key.append(False)  # Positive values are sorted before negative ones
                key.append(value)
            else:
                key.append(True)
                key.append('')
        return key


def create_search_order_type(name: str, fields: Tuple[Field]) -> Type:
    field_names_enum = create_search_order_field_names_enum(name, fields)
    attrs = dict(__annotations__=dict(field_names=Tuple[field_names_enum, ...]))
    sortable = type(f"{name}SortOrder", (SearchOrder,), attrs)
    sortable = dataclass(sortable)
    return sortable


def create_search_order_field_names_enum(name: str, fields: Tuple[Field]):
    attrs = {f.name.upper(): f.name for f in fields if get_optional_type(f.type)}
    sortable = type(f"{name}Sortable", (Enum,), attrs)
    return sortable


def is_sortable(cls: Type):
    return cls.__lt__ != object.__lt__ or cls.__gt__ != object.__gt__
