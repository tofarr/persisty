from typing import Optional, Tuple, Any

from sqlalchemy import Table, or_

from persisty.impl.sqlalchemy.search_filter.and_filter_converter import (
    AndFilterConverter,
)
from persisty.search_filter.or_filter import Or
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store_meta import StoreMeta


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
