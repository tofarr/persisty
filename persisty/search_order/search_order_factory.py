from dataclasses import dataclass
from enum import Enum
from typing import Type, Optional

from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr
from persisty.store_meta import StoreMeta
from persisty.util import to_camel_case


class SearchOrderFactoryABC:
    """
    A lot of formats rely on search order having a simple flat structure. This facilitates that pattern
    """

    attr: Enum
    desc: bool

    def to_search_order(self) -> SearchOrder:
        attr = self.attr
        if attr:
            return SearchOrder((SearchOrderAttr(self.attr.name, self.desc),))


def search_order_dataclass_for(
    store_meta: StoreMeta,
) -> Optional[Type[SearchOrderFactoryABC]]:
    sortable_attrs = store_meta.get_sortable_attrs_as_enum()
    if not sortable_attrs:
        return
    params = {
        "__annotations__": {
            "attr": Optional[sortable_attrs],
            "desc": bool,
        },
        "attr": None,
        "desc": False,
    }
    name = f"{to_camel_case(store_meta.name)}SearchOrder"
    type_ = dataclass(type(name, (SearchOrderFactoryABC,), params))
    return type_
