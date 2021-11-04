import itertools
from dataclasses import dataclass
from typing import TypeVar, Generic, Type, List, Optional, Iterator

from marshy import get_default_context
from marshy.marshaller.marshaller_abc import MarshallerABC

from persisty.errors import PersistyError
from persisty.item_comparator import ItemComparatorABC, MultiComparator, AttrComparator
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.page import Page
from persisty.search_filter import SearchFilter
from persisty.sql.sql_table import SqlTable
from persisty.sql.where import build_where, Where

T = TypeVar('T')


@dataclass(frozen=True)
class Searcher(Generic[T]):
    """ Models the select and from clause of the query """
    _sql: str
    _count_sql: str
    _sql_table: SqlTable
    marshaller: MarshallerABC[T]

    def search(self, cursor, search_filter: Optional[SearchFilter[T]] = None, batch_size: int = 100) -> Iterator[T]:
        where = self._build_select_where(search_filter)
        try:
            cursor.execute(where.sql, where.params)
            while True:
                rows = cursor.fetchmany(size=batch_size)
                if not rows:
                    return
                for row in rows:
                    item = self._read_row(row)
                    yield item
        except Exception as e:
            raise PersistyError(str(e))

    def paged_search(self, cursor, search_filter: Optional[SearchFilter[T]] = None, page_key: Optional[str] = None,
                     limit: int = 20):
        where = self._build_select_where(search_filter)
        sql = f"{where.sql} LIMIT {limit}"
        if page_key:
            page_key = int(page_key)
            sql = f"{sql} OFFSET {int(page_key)}"
        else:
            page_key = 0
        try:
            cursor.execute(sql, where.params)
            rows = cursor.fetchmany(size=limit)
            items = [self._read_row(row) for row in rows]
            page_key += len(items)
            return Page(items, str(page_key))
        except Exception as e:
            raise PersistyError(str(e))

    def count(self, cursor, item_filter: Optional[ItemFilterABC[T]] = None):
        where = build_where(item_filter, self._sql_table)
        sql = self._count_sql
        if where.sql:
            sql = f'{sql} WHERE {where.sql}'
        try:
            cursor.execute(sql, where.params)
            row = cursor.fetchone()
            return row[0]
        except Exception as e:
            raise PersistyError(str(e))

    def _read_row(self, row: List) -> T:
        values = {col.name: row[index] for index, col in enumerate(self._sql_table.cols)}
        loaded = self.marshaller.load(values)
        return loaded

    @property
    def type(self):
        return self.marshaller.marshalled_type

    def _build_select_where(self, search_filter: Optional[SearchFilter] = None):
        where = build_where(search_filter.item_filter if search_filter else None, self._sql_table)
        sql = self._sql
        if where.sql:
            sql = f'{sql} WHERE {where.sql}'
        order = self._build_order(search_filter.item_comparator if search_filter else None)
        if order:
            sql = f"{sql} ORDER BY {','.join(order)}"
        return Where(sql, where.params, where.pure_sql)

    def _build_order(self, comparator: Optional[ItemComparatorABC[T]]) -> List[str]:
        if isinstance(comparator, MultiComparator):
            orders = (self._build_order(c) for c in comparator.comparators)
            orders = list(itertools.chain(*orders))
            return orders
        elif isinstance(comparator, AttrComparator):
            property_ = next((c for c in self._sql_table.cols if c.name == comparator.attr), None)
            if property_:
                return [comparator.attr]
        return []


def searcher(sql_table: SqlTable, item_type: Type[T]):
    return Searcher(
        _sql=f"SELECT {','.join(c.name for c in sql_table.cols)} FROM {sql_table.name}",
        _count_sql=f'SELECT COUNT(*) FROM {sql_table.name}',
        _sql_table=sql_table,
        marshaller=get_default_context().get_marshaller(item_type)
    )
