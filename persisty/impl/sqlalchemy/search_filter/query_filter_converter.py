from operator import or_
from typing import Optional, Tuple, Any

from sqlalchemy import Table

from persisty.attr.attr_type import AttrType
from persisty.impl.sqlalchemy.search_filter.and_filter_converter import (
    AndFilterConverter,
)
from persisty.search_filter.query_filter import QueryFilter
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store_meta import StoreMeta

_NULL = None


class QueryFilterConverter(AndFilterConverter):
    def convert(
        self,
        search_filter: SearchFilterABC,
        table: Table,
        store_meta: StoreMeta,
        context,
    ) -> Optional[Tuple[Any, bool]]:
        if isinstance(search_filter, QueryFilter):
            sub_clauses = []
            for attr in store_meta.attrs:
                if attr.type is AttrType.STR and attr.is_readable:
                    col = table.columns.get(attr.name)
                    sub_clauses.append(col.contains(search_filter.query))
            return or_(*sub_clauses), True
