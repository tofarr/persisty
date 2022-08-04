from dataclasses import dataclass
from enum import Enum
from typing import Type, Optional

from schemey import schema_from_type

from persisty.field.field_filter import FieldFilter
from persisty.field.field_type import FieldType
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.query_filter import QueryFilter
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_field import SearchOrderField
from persisty.storage.storage_meta import StorageMeta
from persisty.util import to_camel_case


class SearchOrderFactoryABC:
    """
    A lot of formats rely on search order having a simple flat structure. This facilitates that pattern
    """

    field: Enum
    desc: bool

    def to_search_order(self) -> SearchOrder:
        field = self.field
        if field:
            return SearchOrder((SearchOrderField(self.field.name, self.desc),))


def search_order_dataclass_for(
    storage_meta: StorageMeta,
) -> Type[SearchOrderFactoryABC]:
    params = {
        "__annotations__": {
            "field": Optional[storage_meta.get_sortable_fields_as_enum()],
            "desc": bool,
        },
        "field": None,
        "desc": False,
    }
    name = f"{to_camel_case(storage_meta.name)}SearchOrder"
    type_ = dataclass(type(name, (SearchOrderFactoryABC,), params))
    return type_
