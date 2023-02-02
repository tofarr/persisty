from dataclasses import dataclass
from typing import Type, Optional

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_type import AttrType
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.query_filter import QueryFilter
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store_meta import StoreMeta
from persisty.util import to_camel_case


class SearchFilterFactoryABC:
    """
    A lot of formats rely on search filters having a simple flat structure. This facilitates that pattern
    """

    __persisty_store_meta__: StoreMeta

    def to_search_filter(self) -> SearchFilterABC:
        search_filter = INCLUDE_ALL
        query = getattr(self, "query", None)
        if query:
            search_filter = QueryFilter(query)
        for attr in self.__persisty_store_meta__.attrs:
            for op in attr.permitted_filter_ops:
                filter_name = f"{attr.name}__{op.name}"
                value = getattr(self, filter_name, None)
                if value is not None:
                    search_filter &= AttrFilter(attr.name, op, value)
        return search_filter


def search_filter_dataclass_for(
    storage_meta: StoreMeta,
) -> Optional[Type[SearchFilterFactoryABC]]:
    annotations = {}
    has_str = next(
        (True for f in storage_meta.attrs if f.attr_type == AttrType.STR), False
    )
    if has_str:
        annotations["query"] = Optional[str]
    for attr in storage_meta.attrs:
        attr_type = Optional[attr.schema.python_type]
        for op in attr.permitted_filter_ops:
            annotations[f"{attr.name}__{op.name}"] = attr_type
    if not annotations:
        return
    params = {k: None for k in annotations}
    params["__persisty_store_meta__"] = storage_meta
    params["__annotations__"] = annotations
    name = f"{to_camel_case(storage_meta.name)}SearchFilter"
    type_ = dataclass(type(name, (SearchFilterFactoryABC,), params))
    return type_
