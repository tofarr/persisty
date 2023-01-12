from typing import Optional, Tuple, Any
from uuid import UUID

from sqlalchemy import Table, Column

from persisty.attr.attr_filter import AttrFilter, AttrFilterOp
from persisty.impl.sqlalchemy.search_filter.and_filter_converter import (
    AndFilterConverter,
)
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store_meta import StoreMeta

_NULL = None


class AttrFilterConverter(AndFilterConverter):
    def convert(
        self,
        search_filter: SearchFilterABC,
        table: Table,
        store_meta: StoreMeta,
        context,
    ) -> Optional[Tuple[Any, bool]]:
        if isinstance(search_filter, AttrFilter):
            col = table.columns.get(search_filter.name)
            clause = self.get_clause(col, search_filter.op, search_filter.value)
            return clause, True

    @staticmethod
    def get_clause(col: Column, op: AttrFilterOp, value: Any):
        """
        Since we are building sqlalchemy where clauses here - the syntax is a little misleading. col == value
        does not do a direct comparison, but rather produces a where clause for comparing the contents of a column
        to the value given.
        """
        if isinstance(value, UUID):
            value = str(value)
        if op == AttrFilterOp.contains:
            return col.contains(value)
        if op == AttrFilterOp.endswith:
            return col.endswith(value)
        if op == AttrFilterOp.eq:
            return col == value
        if op == AttrFilterOp.exists:
            return col != _NULL  # yields col IS NOT NULL
        if op == AttrFilterOp.gt:
            return col > value
        if op == AttrFilterOp.gte:
            return col >= value
        if op == AttrFilterOp.lt:
            return col < value
        if op == AttrFilterOp.lte:
            return col <= value
        if op == AttrFilterOp.ne:
            return col != value
        if op == AttrFilterOp.not_exists:
            return col == _NULL  # yields col IS NULL
        if op == AttrFilterOp.oneof:
            return col.in_(*value)
        if op == AttrFilterOp.startswith:
            return col.startswith(value)
