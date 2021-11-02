import itertools
from dataclasses import dataclass
from typing import Optional, Iterator, ForwardRef, Type, Union, Iterable, Sized, Callable, Any, List

from marshy.marshaller.marshaller_abc import MarshallerABC

from persisty.capabilities import Capabilities, ALL_CAPABILITIES
from persisty.item_comparator import ItemComparatorABC, MultiComparator, AttrComparator
from persisty.item_filter import AttrFilter, AttrFilterOp
from persisty.item_filter.and_filter import AndFilter
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.item_filter.not_filter import NotFilter
from persisty.item_filter.or_filter import OrFilter
from persisty.item_filter.query_filter import QueryFilter
from persisty.page import Page
from persisty.schema.string_schema import StringSchema
from persisty.search_filter import SearchFilter
from persisty.sql.where import Where
from persisty.store.store_abc import StoreABC, T
from persisty.store_schemas import StoreSchemas

@dataclass
class TableStore(StoreABC[T]):
    get_cursor: Callable[[], Any]
    store_name: str
    item_marshaller: MarshallerABC[T]
    key_column: str
    store_schemas: StoreSchemas
    table_name: str = None
    generate_keys: bool = True
    batch_size: int = 100

    @property
    def name(self) -> str:
        return self.store_name

    @property
    def item_type(self) -> Type[T]:
        return self.item_marshaller.marshalled_type

    @property
    def capabilities(self) -> Capabilities:
        return ALL_CAPABILITIES

    @property
    def schemas(self) -> ForwardRef('StoreSchemas[T]'):
        return self.store_schemas

    def get_key(self, item: T) -> str:
        return getattr(item, self.key_column)

    def create(self, item: T) -> str:
        columns = [p for p in self.store_schemas.read.property_schemas
                   if not self.generate_keys or p.name != self.key_column]
        keys = ','.join(c.name for c in columns)
        values_sql = ','.join('?' for c in columns)
        sql = f"INSERT INTO {self.table_name} ({keys}) VALUES ({values_sql})"
        values = [getattr(item, c.name) for c in columns]
        with self.get_cursor() as cursor:
            cursor.execute(sql, values)
            if self.generate_keys:
                key = cursor.lastrowid
                setattr(item, self.key_column, key)
            else:
                key = getattr(item, self.key_column)
            return key

    def read(self, key: str) -> Optional[T]:
        sql = f"{self._build_select()} WHERE {self.key_column}=?"
        with self.get_cursor() as cursor:
            cursor.execute(sql, (key,))
            row = cursor.fetchone()
            item = self._row_to_item(row)
            return item

    def _build_select(self):
        return f"SELECT {','.join(p.name for p in self.store_schemas.read.property_schemas)} FROM {self.table_name}"

    def _row_to_item(self, row):
        kwargs = {p.name: row[index] for index, p in enumerate(self.store_schemas.read.property_schemas)}
        item = self.item_marshaller.load(kwargs)
        return item

    def update(self, item: T) -> T:
        columns = [p for p in self.store_schemas.read.property_schemas if p.name != self.key_column]
        sql = f"UPDATE {self.table_name} SET {','.join(f'{c.name}=?' for c in columns)} WHERE {self.key_column}=?"
        values = [getattr(item, c.name) for c in columns]
        values.append(getattr(item, self.key_column))
        with self.get_cursor() as cursor:
            cursor.execute(sql, values)
        return item

    def destroy(self, key: str) -> bool:
        sql = f"DELETE FROM {self.table_name} WHERE {self.key_column}=?"
        with self.get_cursor() as cursor:
            cursor.execute(sql, (key,))
            destroyed = cursor.rowcount != 0
            return destroyed

    def search(self, search_filter: Optional[SearchFilter[T]] = None) -> Iterator[T]:
        sql = self._build_select()
        where = self._build_where(search_filter.item_filter if search_filter else None)
        if where.sql:
            sql = f'{sql} WHERE {where.sql}'
        order = self._build_order(search_filter.item_comparator if search_filter else None)
        if order:
            sql = f"{sql} ORDER BY {','.join(order)}"
        with self.get_cursor() as cursor:
            cursor.execute(sql, where.params)
            while True:
                rows = cursor.fetchmany(size=self.batch_size)
                if not rows:
                    return
                for row in rows:
                    item = self._row_to_item(row)
                    yield item

    def _build_where(self, item_filter: Optional[ItemFilterABC[T]] = None) -> Where:
        """ Translate as much of te filter given as possible into SQL """
        if item_filter is None:
            return Where('', [], True)
        if isinstance(item_filter, AndFilter):
            wheres = [self._build_where(f) for f in item_filter.filters]
            where = Where(
                sql=f"({' AND '.join(w.sql for w in wheres if w.sql)})",
                params=tuple(itertools.chain(w.params for w in wheres)),
                pure_sql=next((w.pure_sql for w in wheres if w.pure_sql is False), False)
            )
            return where
        elif isinstance(item_filter, OrFilter):
            wheres = [self._build_where(f) for f in item_filter.filters]
            where = Where(
                sql=f"({' OR '.join(w.sql for w in wheres if w.sql)})",
                params=tuple(itertools.chain(w.params for w in wheres)),
                pure_sql=next((w.pure_sql for w in wheres if w.pure_sql is False), False)
            )
            return where
        elif isinstance(item_filter, NotFilter):
            where = self._build_where(item_filter.filter)
            where = Where(f'NOT {where.sql}', where.params, where.pure_sql)
            return where
        elif isinstance(item_filter, AttrFilter):
            attr = item_filter.attr
            property = next((p for p in self.store_schemas.read.property_schemas if p.name == attr), None)
            if not property:
                return Where('', tuple(), False)
            op = item_filter.op
            value = item_filter.value
            if op == AttrFilterOp.contains:
                return Where(f'{attr} like ?', [f'%{value}%'], True)
            if op == AttrFilterOp.endswith:
                return Where(f'{attr} like ?', [f'%{value}'], True)
            if op == AttrFilterOp.eq:
                return Where(f"{attr} = ?", value, True)
            if op == AttrFilterOp.gt:
                return Where(f"{attr} > ?", value, True)
            if op == AttrFilterOp.gte:
                return Where(f"{attr} >= ?", value, True)
            if op == AttrFilterOp.lt:
                return Where(f"{attr} < ?", value, True)
            if op == AttrFilterOp.lte:
                return Where(f"{attr} <= ?", value, True)
            if op == AttrFilterOp.ne:
                return Where(f"{attr} <> ?", value, True)
            if op == AttrFilterOp.oneof:
                return Where(f"{attr} in ({','.join('?' for v in value)})", value, True)
            if op == AttrFilterOp.startswith:
                return Where(f'{attr} like ?', [f'{value}%'], True)
        elif isinstance(item_filter, QueryFilter):
            properties = [p for p in self.store_schemas.read.property_schemas
                          if isinstance(p.schema, StringSchema) and not p.name.endswith('_id')]
            return Where(
                sql=f"({' OR '.join(f'{p.name} like ?' for p in properties)})",
                params=[item_filter.query for p in properties],
                pure_sql=True
            )
        else:
            return Where('', tuple(), False)

    def _build_order(self, comparator: Optional[ItemComparatorABC[T]]) -> List[str]:
        if isinstance(comparator, MultiComparator):
            orders = (self._build_order(c) for c in comparator.comparators)
            orders = list(itertools.chain(*orders))
            return orders
        elif isinstance(comparator, AttrComparator):
            property = next((p for p in self.store_schemas.read.property_schemas if p.name == comparator.attr), None)
            if property:
                return [comparator.attr]
        return []

    def count(self, search_filter: Optional[SearchFilter[T]] = None) -> int:
        sql = f'SELECT COUNT(*) FROM {self.table_name}'
        where = self._build_where(search_filter.item_filter if search_filter else None)
        if where.sql:
            sql = f'{sql} WHERE {where.sql}'
        with self.get_cursor() as cursor:
            cursor.execute(sql, where.params)
            row = cursor.fetchone()
            return row[0]

    def paged_search(self, search_filter: Optional[SearchFilter[T]] = None, page_key: Optional[str] = None,
                     limit: int = 20) -> Page[T]:
        sql = self._build_select()
        where = self._build_where(search_filter.item_filter if search_filter else None)
        if where.sql:
            sql = f'{sql} WHERE {where.sql}'
        order = self._build_order(search_filter.item_comparator if search_filter else None)
        if order:
            sql = f"{sql} ORDER BY {','.join(order)}"
        sql = f"{sql} LIMIT {limit}"
        if page_key:
            page_key = int(page_key)
            sql = f"{sql} OFFSET {int(page_key)}"
        else:
            page_key = 0
        with self.get_cursor() as cursor:
            cursor.execute(sql, where.params)
            rows = cursor.fetchmany(size=limit)
            items = [self._row_to_item(row) for row in rows]
            page_key += len(items)
            return Page(items, str(page_key))