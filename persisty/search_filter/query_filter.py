from __future__ import annotations
from typing import Tuple, Optional, Any, TYPE_CHECKING

from dataclasses import dataclass

from persisty.attr.attr_filter import AttrFilter, AttrFilterOp
from persisty.attr.attr_type import AttrType
from persisty.search_filter.or_filter import Or
from persisty.search_filter.search_filter_abc import SearchFilterABC, T

if TYPE_CHECKING:
    from persisty.attr.attr import Attr


@dataclass
class QueryFilter(SearchFilterABC[T]):
    query: str

    def __post_init__(self):
        object.__setattr__(self, "query", self.query.lower())

    def lock_attrs(self, attrs: Tuple[Attr, ...]) -> SearchFilterABC:
        filters = []
        for attr in attrs:
            if attr.readable and attr.type is AttrType.STR:
                filters.append(AttrFilter(attr.name, AttrFilterOp.contains, self.query))
        return Or(tuple(filters))

    def match(self, item: T, attrs: Tuple[Attr, ...]) -> bool:
        for attr in attrs:
            if not attr.readable or attr.type is not AttrType.STR:
                continue
            attr_value = item.get(attr.name)
            if isinstance(attr_value, str):
                if self.query in attr_value.lower():
                    return True
        return False

    def build_filter_expression(
        self, attrs: Tuple[Attr, ...]
    ) -> Tuple[Optional[Any], bool]:
        conditions = []
        for attr in attrs:
            if not attr.readable or attr.type is not AttrType.STR:
                continue
            from boto3.dynamodb.conditions import Attr

            conditions.append(Attr(attr.name).contains(self.query))
        if not conditions:
            return None, False
        if len(conditions) == 1:
            condition = conditions[0]
        else:
            from boto3.dynamodb.conditions import Or as DynOr

            condition = DynOr(*conditions)
        return condition, True
