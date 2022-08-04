from dataclasses import dataclass
from typing import Type, Optional

from schemey import schema_from_type

from persisty.field.field_filter import FieldFilter
from persisty.field.field_type import FieldType
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.query_filter import QueryFilter
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.storage_meta import StorageMeta
from persisty.util import to_camel_case


class SearchFilterFactoryABC:
    """
    A lot of formats rely on search filters having a simple flat structure. This facilitates that pattern
    """

    __persisty_storage_meta__: StorageMeta

    def to_search_filter(self) -> SearchFilterABC:
        search_filter = INCLUDE_ALL
        query = getattr(self, "query", None)
        if query:
            search_filter = QueryFilter(query)
        for field in self.__persisty_storage_meta__.fields:
            for op in field.permitted_filter_ops:
                filter_name = f"{field.name}__{op.name}"
                value = getattr(self, filter_name, None)
                if value is not None:
                    search_filter &= FieldFilter(field.name, op, value)
        return search_filter


def search_filter_dataclass_for(
    storage_meta: StorageMeta,
) -> Type[SearchFilterFactoryABC]:
    annotations = {}
    has_str = next(
        (True for f in storage_meta.fields if f.type == FieldType.STR), False
    )
    if has_str:
        annotations["query"] = Optional[str]
    for field in storage_meta.fields:
        field_type = Optional[field.schema.python_type]
        for op in field.permitted_filter_ops:
            annotations[f"{field.name}__{op.name}"] = field_type
    params = {k: None for k in annotations}
    params["__persisty_storage_meta__"] = storage_meta
    params["__annotations__"] = annotations
    name = f"{to_camel_case(storage_meta.name)}SearchFilter"
    type_ = dataclass(type(name, (SearchFilterFactoryABC,), params))
    return type_
