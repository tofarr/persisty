from typing import Optional, Tuple, Any

from sqlalchemy import Table, not_

from persisty.impl.sqlalchemy.search_filter.and_filter_converter import (
    AndFilterConverter,
)
from persisty.search_filter.not_filter import Not
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store_meta import StoreMeta


class NotFilterConverter(AndFilterConverter):
    def convert(
        self,
        search_filter: SearchFilterABC,
        table: Table,
        store_meta: StoreMeta,
        context,
    ) -> Optional[Tuple[Any, bool]]:
        if isinstance(search_filter, Not):
            clause, handled = context.convert(
                search_filter.search_filter, table, store_meta
            )
            if clause:
                clause = not_(clause)
            return clause, handled
