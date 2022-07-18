import dataclasses
from abc import ABC

from persisty.storage.field.field_filter import FieldFilter
from persisty.storage.search_filter import SearchFilterABC, QueryFilter, And
from persisty.util import UNDEFINED


class ItemFilterOp:
    pass


class SearchFilterFactoryABC(ABC):

    def to_search_filter(self) -> SearchFilterABC:
        """ Convert this search filter factory to a search filter instance """
        search_filters = []
        value = getattr(self, "query", UNDEFINED)
        if value:
            search_filters.append(QueryFilter(value))
        for field in dataclasses.fields(self):
            name = field.name
            value = getattr(self, name)
            if value in (None, UNDEFINED, ""):
                continue
            for op in ItemFilterOp:
                if name.endswith(f"__{op.name}"):
                    attr_name = name[:-(len(op.name)+2)]
                    search_filters.append(FieldFilter(attr_name, op, value))
                    break
        return And(search_filters)
