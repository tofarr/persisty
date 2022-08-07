import json
from typing import Optional, List, Iterator, Tuple, Any

from dataclasses import dataclass, field
from marshy.types import ExternalItemType
from sqlalchemy.orm import Session, sessionmaker

from persisty.field.field import Field
from persisty.field.field_type import FieldType
from persisty.impl.sqlalchemy.search_filter.search_filter_converter_context import SearchFilterConverterContext
from persisty.impl.sqlalchemy.sqlalchemy_column_converter import POSTGRES
from persisty.impl.sqlalchemy.sqlalchemy_connector import get_default_session_maker
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.storage.batch_edit import BatchEdit
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta

from sqlalchemy import (Table, and_, select, func, Column)

from persisty.util import UNDEFINED, from_base64, to_base64


@dataclass(frozen=True)
class SqlalchemyTableStorage(StorageABC):
    """
    This class uses sql alchemy at a lower level than the standard orm usage
    """
    storage_meta: StorageMeta
    # SqlAlchemy engine class
    table: Table
    session_maker: sessionmaker = field(default_factory=get_default_session_maker)
    insert_stmt: Optional[Any] = None
    read_stmt: Optional[Any] = None
    delete_stmt: Optional[Any] = None

    def __post_init__(self):
        if not self.insert_stmt:
            insert_stmt = self.table.insert
            object.__setattr__(self, 'insert_stmt', insert_stmt)
        if not self.read_stmt:
            read_stmt = self.table.select(whereclause=self._key_where_clause())
            object.__setattr__(self, 'read_stmt', read_stmt)
        if not self.delete_stmt:
            delete_stmt = self.table.delete(whereclause=self._key_where_clause())
            object.__setattr__(self, 'delete_stmt', delete_stmt)

    def get_storage_meta(self) -> StorageMeta:
        return self.storage_meta

    def create(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        with self.session_maker() as session:
            dumped = self._dump(item, False, session)
            result = session.execute(self.insert_stmt, *dumped)
            if result.inserted_primary_key:
                for column, value in zip(
                    self.table.primary_key, result.inserted_primary_key
                ):
                    item[column.name] = value
            return item

    def read(self, key: str) -> Optional[ExternalItemType]:
        with Session(self.session_maker()) as session:
            row = session.execute(self.read_stmt, **self._key_from_str(key)).first()
            item = self._load(row, session)
            return item

    def update(
        self, updates: ExternalItemType, search_filter: SearchFilterABC = INCLUDE_ALL
    ) -> Optional[ExternalItemType]:
        if search_filter is EXCLUDE_ALL:
            return None
        where_clause = self._key_where_clause_from_item(updates)
        search_filter_where_clause, handled = self._search_filter_to_where_clause(
            search_filter
        )
        if search_filter_where_clause:
            where_clause = and_(where_clause, search_filter_where_clause)
        with self.session_maker() as session:
            values = self._dump(updates, True, session)
            stmt = self.table.update(whereclause=where_clause, values=values)
            rows = session.execute(stmt)
            for row in rows:
                item = self._load(row, session)
                return item

    def delete(self, key: str) -> bool:
        with Session(self.session_maker()) as session:
            row = session.execute(self.delete_stmt)
            return bool(row[0])

    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        if search_filter is EXCLUDE_ALL:
            return 0
        where_clause, handled = self._search_filter_to_where_clause(search_filter)
        if not handled:
            count = sum(1 for _ in self.search_all(search_filter))
            return count
        with Session(self.session_maker()) as session:
            stmt = select([func.count()]).select_from(self.table)
            if where_clause:
                stmt = stmt.where(where_clause)
            row = session.execute(stmt)
            return bool(row[0])

    def search(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[ExternalItemType]:
        where_clause, handled = self._search_filter_to_where_clause(search_filter)
        order_by = self._search_order_to_order_by(search_order)
        if not order_by:
            order_by = self._default_order_by()
        offset = 0
        if page_key:
            page_key = from_base64(page_key)
            if page_key[0] == 'offset':
                offset = page_key[1]
            else:
                # noinspection PyTypeChecker
                key_where_clause = and_(
                    self.table.columns.get(f.name) > page_key[1][f.name]
                    for f in self._key_fields()
                )
                where_clause = and_(where_clause, key_where_clause) if where_clause else key_where_clause

        with self.session_maker() as session:
            stmt = self.table.select()
            if where_clause:
                stmt = stmt.where(where_clause)
            stmt = stmt.order_by(order_by)
            if offset:
                stmt = stmt.offset(offset)
            if handled:
                stmt = stmt.limit(limit)

            rows = session.execute(stmt)
            results = []
            next_page_key = None
            for row in rows:
                result = self._load(row, session)
                if handled or search_filter.match(result, self.storage_meta.fields):
                    results.append(result)
                    if len(results) == limit:
                        if search_order and search_order.orders:
                            next_page_key = ['offset', offset + len(results)]
                        if not search_order or not search_order.orders:
                            next_page_key = ['id', {
                                f.name: result[f.name]
                                for f in self._key_fields()
                            }]
                        next_page_key = to_base64(next_page_key)
                        break
            return ResultSet(results, next_page_key)

    def search_all(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
    ) -> Iterator[ExternalItemType]:
        if search_filter is EXCLUDE_ALL:
            return ResultSet([])
        where_clause, handled = self._search_filter_to_where_clause(search_filter)
        order_by = self._search_order_to_order_by(search_order)
        with self.session_maker() as session:
            stmt = self.table.select()
            if where_clause:
                stmt = stmt.where(where_clause)
            if order_by:
                stmt = stmt.order_by(order_by)
            rows = session.execute(stmt)
            for row in rows:
                item = self._load(row, session)
                if not handled and not search_filter.match(item, self.storage_meta.fields):
                    continue
                yield item

    def edit_batch(self, edits: List[BatchEdit]) -> List[BatchEditResult]:
        results_by_id = {}
        inserts = [e for e in edits if e.create_item]
        updates = [e for e in edits if e.update_item]
        deletes = [e for e in edits if e.delete_key]
        with self.session_maker() as session:
            if inserts:
                stmt = self.table.insert
                for insert in inserts:
                    stmt.execute(self._dump(insert.create_item, False, session))
                    results_by_id[insert.id] = BatchEditResult(insert, True)
            if updates:
                stmt = self.table.update
                for update in updates:
                    stmt.execute(self._dump(update.create_item, False, session))
                    results_by_id[update.id] = BatchEditResult(update, True)
            if deletes:
                key_fields = list(self._key_fields())
                delete_keys = [self.storage_meta.key_config.from_key_str(d.delete_key) for d in deletes]
                if len(key_fields) == 1:
                    delete_keys = [d[key_fields[0].name] for d in delete_keys]
                    stmt = self.table.delete(self.table.columns.get(key_fields[0].name).in_(delete_keys))
                    session.execute(stmt)
                else:
                    stmt = self.table.delete.where(self._key_where_clause())
                    for delete_key in delete_keys:
                        session.execute(stmt, delete_key)
                for delete in deletes:
                    results_by_id[update.id] = BatchEditResult(delete, True)
        results = [results_by_id[e.id] for e in edits]
        return results

    def _load(self, item: ExternalItemType, session: Session):
        loaded = {}
        for field_ in self.storage_meta.fields:
            if field_.name not in item:
                continue
            value = item.get(field_.name)
            if (
                field_.type == FieldType.JSON
                and not session.bind.dialect.name == POSTGRES
            ):
                value = json.loads(value)
            loaded[field_.name] = value
        return loaded

    def _dump(self, item: ExternalItemType, is_update: bool, session: Session):
        dumped = {}
        for field_ in self.storage_meta.fields:
            value = item.get(field_.name, UNDEFINED)
            if field_.write_transform:
                value = field_.write_transform.transform(value, is_update)
                item[field_.name] = value
            if (
                field_.type == FieldType.JSON
                and not session.bind.dialect.name == POSTGRES
            ):
                value = json.dumps(value)
            if value is not UNDEFINED:
                dumped[field_.name] = value
        return dumped

    def _key_fields(self) -> Iterator[Field]:
        for field_ in self.storage_meta.fields:
            if self.storage_meta.key_config.is_required_field(field_.name):
                yield field

    def _key_where_clause(self):
        key_where_clause = and_([
            self.table.columns.get(f.name) == f':{f.name}'
            for f in self._key_fields()
        ])
        return key_where_clause

    def _key_from_str(self, key: str):
        return self.storage_meta.key_config.from_key_str(key)

    def _key_where_clause_from_item(self, item: ExternalItemType):
        key_where_clause = and_(
            self.table.columns.get(field_.name) == item[field_.name]
            for field_ in self._key_fields()
        )
        return key_where_clause

    def _search_filter_to_where_clause(self, search_filter: SearchFilterABC) -> Tuple[Any, bool]:
        search_filter.validate_for_fields(self.storage_meta.fields)
        context = SearchFilterConverterContext()
        return context.convert(search_filter, self.table, self.storage_meta)

    def _search_order_to_order_by(self, search_order: SearchOrder):
        if not search_order:
            return
        search_order.validate_for_fields(self.storage_meta.fields)
        orders = []
        for order_field in search_order.orders:
            order = self.table.columns.get(order_field.field)
            if order_field.desc:
                order = order.desc()
            orders.append(order)
        return orders

    def _default_order_by(self) -> List[Column]:
        orders = [self.table.columns.get(f.name) for f in self._key_fields()]
        return orders
