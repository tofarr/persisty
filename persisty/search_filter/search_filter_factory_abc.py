from __future__ import annotations
import dataclasses
from abc import ABC
from typing import TYPE_CHECKING

from persisty.util import UNDEFINED

if TYPE_CHECKING:
    from persisty.search_filter.search_filter_abc import SearchFilterABC


class SearchFilterFactoryABC(ABC):
    def to_search_filter(self) -> SearchFilterABC:
        """Convert this search filter factory to a search filter instance"""
        from persisty.storage.field.field_filter import FieldFilter, FieldFilterOp

        search_filters = []
        value = getattr(self, "query", UNDEFINED)
        if value:
            from persisty.search_filter.query_filter import QueryFilter

            search_filters.append(QueryFilter(value))
        # noinspection PyDataclass
        for field in dataclasses.fields(self):
            name = field.name
            value = getattr(self, name)
            if value in (None, UNDEFINED, ""):
                continue
            for op in FieldFilterOp:
                if name.endswith(f"__{op.name}"):
                    attr_name = name[: -(len(op.name) + 2)]
                    search_filters.append(FieldFilter(attr_name, op, value))
                    break
        from persisty.search_filter.and_filter import And

        return And(search_filters)
