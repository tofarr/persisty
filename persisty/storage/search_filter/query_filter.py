from typing import Tuple

from dataclasses import dataclass

from marshy.types import ExternalItemType

from persisty.storage.field.field import Field
from persisty.storage.field.field_type import FieldType
from persisty.storage.search_filter.search_filter_abc import SearchFilterABC


@dataclass
class QueryFilter(SearchFilterABC):
    query: str

    def __post_init__(self):
        object.__setattr__(self, 'query', self.query.lower())

    def validate_for_fields(self, fields: Tuple[Field, ...]):
        for field in fields:
            if field.is_readable and field.type is FieldType.STR:
                return
        raise ValueError('query_filter_invalid_for_fields')

    def match(self, item: ExternalItemType, fields: Tuple[Field, ...]) -> bool:
        for field in fields:
            if not field.is_readable or field.type is not FieldType.STR:
                continue
            field_value = item.get(field.name)
            if isinstance(field_value, str):
                if self.query in field_value.lower():
                    return True
        return False
