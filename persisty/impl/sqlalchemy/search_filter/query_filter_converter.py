from operator import or_
from typing import Optional, Tuple, Any

from sqlalchemy import Table

from persisty.field.field_type import FieldType
from persisty.impl.sqlalchemy.search_filter.and_filter_converter import (
    AndFilterConverter,
)
from persisty.search_filter.query_filter import QueryFilter
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.storage_meta import StorageMeta

_NULL = None


class QueryFilterConverter(AndFilterConverter):
    def convert(
        self,
        search_filter: SearchFilterABC,
        table: Table,
        storage_meta: StorageMeta,
        context,
    ) -> Optional[Tuple[Any, bool]]:
        if isinstance(search_filter, QueryFilter):
            sub_clauses = []
            for field in storage_meta.fields:
                if field.type is FieldType.STR and field.is_readable:
                    col = table.columns.get(field.name)
                    sub_clauses.append(col.contains(search_filter.query))
            return or_(*sub_clauses), True
