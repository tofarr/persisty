from typing import Optional, Tuple, Any

from sqlalchemy import Table, and_

from persisty.impl.sqlalchemy.search_filter.search_filter_converter_abc import (
    SearchFilterConverterABC,
)
from persisty.search_filter.and_filter import And
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.storage_meta import StorageMeta


class AndFilterConverter(SearchFilterConverterABC):
    def convert(
        self,
        search_filter: SearchFilterABC,
        table: Table,
        storage_meta: StorageMeta,
        context,
    ) -> Optional[Tuple[Any, bool]]:
        if isinstance(search_filter, And):
            sub_clauses, handled = self.get_sub_clauses(
                search_filter.search_filters, table, storage_meta, context
            )
            clause = and_(*sub_clauses) if sub_clauses else None
            return clause, handled

    @staticmethod
    def get_sub_clauses(
        search_filters: Tuple[SearchFilterABC],
        table: Table,
        storage_meta: StorageMeta,
        context,
    ) -> Optional[Tuple[Any, bool]]:
        sub_clauses = []
        handled = True
        for sub_filter in search_filters:
            sub_clause, sub_handled = context.convert(sub_filter, table, storage_meta)
            if sub_clause:
                sub_clauses.append(sub_clause)
            handled = handled and sub_handled
        return sub_clauses, handled
