from __future__ import annotations
from typing import Tuple, Optional, Any, TYPE_CHECKING

from dataclasses import dataclass

from marshy.types import ExternalItemType

from persisty.field.field_filter import FieldFilter, FieldFilterOp
from persisty.field.field_type import FieldType
from persisty.search_filter.or_filter import Or
from persisty.search_filter.search_filter_abc import SearchFilterABC

if TYPE_CHECKING:
    from persisty.field.field import Field


@dataclass
class QueryFilter(SearchFilterABC):
    query: str

    def __post_init__(self):
        object.__setattr__(self, "query", self.query.lower())

    def lock_fields(self, fields: Tuple[Field, ...]) -> SearchFilterABC:
        filters = []
        for field in fields:
            if field.is_readable and field.type is FieldType.STR:
                filters.append(FieldFilter(field.name, FieldFilterOp.contains, self.query))
        return Or(tuple(filters))

    def match(self, item: ExternalItemType, fields: Tuple[Field, ...]) -> bool:
        for field in fields:
            if not field.is_readable or field.type is not FieldType.STR:
                continue
            field_value = item.get(field.name)
            if isinstance(field_value, str):
                if self.query in field_value.lower():
                    return True
        return False

    def build_filter_expression(
        self, fields: Tuple[Field, ...]
    ) -> Tuple[Optional[Any], bool]:
        conditions = []
        for field in fields:
            if not field.is_readable or field.type is not FieldType.STR:
                continue
            from boto3.dynamodb.conditions import Attr

            conditions.append(Attr(field.name).contains(self.query))
        if not conditions:
            return None, False
        if len(conditions) == 1:
            condition = conditions[0]
        else:
            from boto3.dynamodb.conditions import Or as DynOr

            condition = DynOr(*conditions)
        return condition, True
