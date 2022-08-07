from typing import Optional, Tuple, Any

from sqlalchemy import Table, Column

from persisty.field.field_filter import FieldFilter, FieldFilterOp
from persisty.impl.sqlalchemy.search_filter.and_filter_converter import (
    AndFilterConverter,
)
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.storage_meta import StorageMeta

_NULL = None


class FieldFilterConverter(AndFilterConverter):
    def convert(
        self, search_filter: SearchFilterABC, table: Table, storage_meta: StorageMeta, context
    ) -> Optional[Tuple[Any, bool]]:
        if isinstance(search_filter, FieldFilter):
            col = table.columns.get(search_filter.name)
            clause = self.get_clause(col, search_filter.op, search_filter.value)
            return clause, True

    @staticmethod
    def get_clause(col: Column, op: FieldFilterOp, value: Any):
        """
        Since we are building sqlalchemy where clauses here - the syntax is a little misleading. col == value
        does not do a direct comparison, but rather produces a where clause for comparing the contents of a column
        to the value given.
        """
        if op == FieldFilterOp.contains:
            return col.contains(value)
        if op == FieldFilterOp.endswith:
            return col.endswith(value)
        if op == FieldFilterOp.eq:
            return col == value
        if op == FieldFilterOp.exists:
            return col != _NULL  # yields col IS NOT NULL
        if op == FieldFilterOp.gt:
            return col > value
        if op == FieldFilterOp.gte:
            return col >= value
        if op == FieldFilterOp.lt:
            return col < value
        if op == FieldFilterOp.lte:
            return col <= value
        if op == FieldFilterOp.ne:
            return col != value
        if op == FieldFilterOp.not_exists:
            return col == _NULL  # yields col IS NULL
        if op == FieldFilterOp.oneof:
            return col.in_(*value)
        if op == FieldFilterOp.startswith:
            return col.startswith(value)
