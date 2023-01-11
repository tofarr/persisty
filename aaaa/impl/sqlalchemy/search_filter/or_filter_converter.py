from typing import Optional, Tuple, Any

from sqlalchemy import Table, or_

from aaaa.impl.sqlalchemy.search_filter.and_filter_converter import (
    AndFilterConverter,
)
from aaaa.search_filter.or_filter import Or
from aaaa.search_filter.search_filter_abc import SearchFilterABC
from aaaa.store_meta import StoreMeta


class OrFilterConverter(AndFilterConverter):
    def convert(
        self,
        search_filter: SearchFilterABC,
        table: Table,
        store_meta: StoreMeta,
        context,
    ) -> Optional[Tuple[Any, bool]]:
        if isinstance(search_filter, Or):
            sub_clauses, handled = self.get_sub_clauses(
                search_filter.search_filters, table, store_meta, context
            )
            clause = or_(*sub_clauses) if sub_clauses else None
            return clause, handled
