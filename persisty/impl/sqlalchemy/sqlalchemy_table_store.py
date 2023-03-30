import json
from datetime import timezone
from typing import Optional, List, Iterator, Tuple, Any, Dict

from dataclasses import dataclass
from uuid import UUID

import marshy
from marshy.types import ExternalItemType
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DatabaseError
from sqlalchemy.sql.elements import BindParameter, or_

from persisty.errors import PersistyError
from persisty.attr.attr_type import AttrType
from persisty.impl.sqlalchemy.search_filter.search_filter_converter_context import (
    SearchFilterConverterContext,
)
from persisty.impl.sqlalchemy.sqlalchemy_column_converter import POSTGRES
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.batch_edit import BatchEdit
from persisty.batch_edit_result import BatchEditResult
from persisty.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.store.store_abc import StoreABC, T
from persisty.store_meta import StoreMeta

from sqlalchemy import Table, and_, select, func, Column

from persisty.util import UNDEFINED, from_base64, to_base64


def catch_db_error(fn):
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except DatabaseError as e:
            raise PersistyError(e)

    return wrapper


@dataclass(frozen=True)
class SqlalchemyTableStore(StoreABC):
    """
    This class uses sql alchemy at a lower level than the standard orm usage
    """

    meta: StoreMeta
    table: Table
    engine: Engine

    def get_meta(self) -> StoreMeta:
        return self.meta

    @catch_db_error
    def create(self, item: T) -> Optional[T]:
        dumped = self._dump(item, False)
        with self.engine.begin() as connection:
            result = connection.execute(self.table.insert(), parameters=dumped)
            if result.inserted_primary_key:
                row = self._load_row(result.inserted_primary_key)
                for attr in self.meta.attrs:
                    value = getattr(row, attr.name)
                    if value is UNDEFINED:
                        setattr(row, attr.name, value)
            connection.commit()
            return item

    @catch_db_error
    def read(self, key: str) -> Optional[T]:
        with self.engine.begin() as connection:
            key_dict = self.meta.key_config.to_key_dict(key)
            return self._read(connection, key_dict)

    def _read(self, connection, key_dict: ExternalItemType) -> Optional[Dict]:
        stmt = self.table.select().where(self._key_where_clause())
        row = connection.execute(stmt, key_dict).first()
        if row:
            item = self._load_row(row)
            return item

    def read_batch(self, keys: List[str]) -> List[Optional[T]]:
        with self.engine.begin() as connection:
            key_config = self.meta.key_config
            stored_dataclass = self.meta.get_stored_dataclass()
            key_objs = [key_config.to_key_dict(key) for key in keys]
            items = self._read_batch(connection, key_objs)
            items_by_key = {key_config.to_key_str(item): item for item in items}
            items = [items_by_key.get(k) for k in keys]
            return items

    def _read_batch(
        self, connection, keys: List[T], cols: Optional[List[Column]] = None
    ) -> List[Dict]:
        # NB: Does not enforce ordering
        assert len(keys) <= self.meta.batch_size
        where_clause = self._key_where_clause_from_dicts(keys)
        if cols:
            stmt = select(*cols)
        else:
            stmt = self.table.select()
        stmt = stmt.where(where_clause)
        results = connection.execute(stmt)
        items = [self._load_row(r) for r in results]
        return items

    @catch_db_error
    def _update(
        self,
        key: str,
        item: T,
        updates: T,
        search_filter: SearchFilterABC = INCLUDE_ALL,
    ) -> Optional[T]:
        with self.engine.begin() as connection:
            stmt = self.table.update(self._key_where_clause_from_item(updates))
            updates = self._dump(updates, True)
            connection.execute(stmt, updates)
            key_dict = self.meta.key_config.to_key_dict(key)
            loaded = self._read(connection, key_dict)
            connection.commit()
            return loaded

    @catch_db_error
    def delete(self, key: str) -> bool:
        with self.engine.begin() as connection:
            key = self.meta.key_config.to_key_dict(key)
            stmt = self.table.delete(whereclause=self._key_where_clause())
            result = connection.execute(stmt, key)
            connection.commit()
            return bool(result.rowcount)

    @catch_db_error
    def _delete(self, key: str, item: T) -> bool:
        return self.delete(key)

    @catch_db_error
    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        if search_filter is EXCLUDE_ALL:
            return 0
        where_clause, handled = self._search_filter_to_where_clause(search_filter)
        if not handled:
            count = sum(1 for _ in self.search_all(search_filter))
            return count
        stmt = select([func.count()]).select_from(self.table)
        if where_clause is not None:
            stmt = stmt.where(where_clause)
        with self.engine.begin() as connection:
            row = connection.execute(stmt).first()
            return row[0]

    @catch_db_error
    def search(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[T]:
        assert limit <= self.meta.batch_size
        where_clause, handled = self._search_filter_to_where_clause(search_filter)
        order_by = self._search_order_to_order_by(search_order)
        if order_by is None:
            order_by = self._default_order_by()
        offset = 0
        if page_key:
            page_key = from_base64(page_key)
            if page_key[0] == "offset":
                offset = page_key[1]
            else:
                # noinspection PyTypeChecker
                key_where_clause = and_(
                    self.table.columns.get(a) > page_key[1][a]
                    for a in self.meta.key_config.get_key_attrs()
                )
                where_clause = (
                    and_(where_clause, key_where_clause)
                    if where_clause is not None
                    else key_where_clause
                )

        stmt = self.table.select()
        if where_clause is not None:
            stmt = stmt.where(where_clause)
        stmt = stmt.order_by(*order_by)
        if offset:
            stmt = stmt.offset(offset)
        if handled:
            stmt = stmt.limit(limit)

        with self.engine.begin() as connection:
            rows = connection.execute(stmt)
            results = []
            next_page_key = None
            for row in rows:
                result = self._load_row(row)
                if handled or search_filter.match(result, self.meta.attrs):
                    results.append(result)
                    if len(results) == limit:
                        if search_order and search_order.orders:
                            next_page_key = ["offset", offset + len(results)]
                        if not search_order or not search_order.orders:
                            next_page_key = [
                                "id",
                                {
                                    a: _transform_type(getattr(result, a))
                                    for a in self.meta.key_config.get_key_attrs()
                                },
                            ]
                        next_page_key = to_base64(next_page_key)
                        break
            return ResultSet(results, next_page_key)

    @catch_db_error
    def search_all(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
    ) -> Iterator[T]:
        if search_filter is EXCLUDE_ALL:
            return ResultSet([])
        where_clause, handled = self._search_filter_to_where_clause(search_filter)
        order_by = self._search_order_to_order_by(search_order)
        stmt = self.table.select()
        if where_clause is not None:
            stmt = stmt.where(where_clause)
        if order_by is not None:
            stmt = stmt.order_by(*order_by)
        with self.engine.begin() as connection:
            rows = connection.execute(stmt)
            for row in rows:
                item = self._load_row(row)
                if not handled and not search_filter.match(item, self.meta.attrs):
                    continue
                yield item

    @catch_db_error
    def edit_batch(self, edits: List[BatchEdit]) -> List[BatchEditResult]:
        assert len(edits) <= self.meta.batch_size
        results_by_id = {}
        inserts = [e for e in edits if e.create_item]
        updates = [e for e in edits if e.update_item]
        deletes = [e for e in edits if e.delete_key]
        with self.engine.begin() as connection:
            if inserts:
                self._batch_insert(connection, inserts, results_by_id)
            if updates:
                self._batch_update(connection, updates, results_by_id)
            if deletes:
                self._batch_delete(connection, deletes, results_by_id)
            results = [results_by_id[e.id] for e in edits]
            connection.commit()
            return results

    def _batch_insert(
        self,
        connection,
        edits: List[BatchEdit],
        results_by_id: Dict[UUID, BatchEditResult],
    ):
        stmt = self.table.insert(bind=connection)
        items_to_create = [self._dump(e.create_item, False) for e in edits]
        connection.execute(stmt, items_to_create)
        for insert in edits:
            results_by_id[insert.id] = BatchEditResult(insert, True)

    def _batch_update(
        self,
        connection,
        edits: List[BatchEdit],
        results_by_id: Dict[UUID, BatchEditResult],
    ):
        edits_by_key = {}
        key_dicts_to_load_row = []
        key_attrs = self.meta.key_config.get_key_attrs()
        to_key_str = self.meta.key_config.to_key_str
        for edit in edits:
            edit_key = to_key_str(edit.update_item)
            edits_by_key[edit_key] = edit
            key_dicts_to_load_row.append(
                {k: _transform_type(getattr(edit.update_item, k)) for k in key_attrs}
            )
            results_by_id[edit.id] = BatchEditResult(edit)
        existing_items = self._read_batch(connection, key_dicts_to_load_row)
        for item in existing_items:
            key = to_key_str(item)
            edit = edits_by_key[key]
            for attr in self.meta.attrs:
                value = UNDEFINED
                if attr.update_generator:
                    if attr.updatable:
                        value = attr.update_generator.transform(
                            getattr(edit.update_item, attr.name)
                        )
                    else:
                        value = attr.update_generator.transform(UNDEFINED)
                elif attr.updatable:
                    value = getattr(edit.update_item, attr.name)
                    if value is UNDEFINED:
                        value = getattr(item, attr.name)
                if value is not UNDEFINED:
                    setattr(item, attr.name, _transform_type(value))
            item_updates = self._dump(item, True)
            stmt = self.table.update(self._key_where_clause_from_item(item))
            connection.execute(stmt, item_updates)
            results_by_id[edit.id].success = True

    def _batch_delete(
        self,
        connection,
        edits: List[BatchEdit],
        results_by_id: Dict[UUID, BatchEditResult],
    ):
        key_config = self.meta.key_config
        delete_keys = [key_config.to_key_dict(d.delete_key) for d in edits]
        existing_keys = self._get_existing_keys(connection, delete_keys)
        where_clause = self._key_where_clause_from_dicts(delete_keys)
        stmt = self.table.delete(whereclause=where_clause)
        connection.execute(stmt)
        deleted_keys = {key_config.to_key_str(k) for k in existing_keys}
        for delete in edits:
            deleted = delete.delete_key in deleted_keys
            results_by_id[delete.id] = BatchEditResult(delete, deleted)

    def _get_existing_keys(self, connection, keys: List[Dict]) -> List[Dict]:
        key_cols = [self.table.columns[a] for a in self.meta.key_config.get_key_attrs()]
        existing_keys = self._read_batch(connection, keys, key_cols)
        return existing_keys

    def _load_row(self, row):
        # Row is a KeyedTuple - _asdict is to match the namedtuple API (it's not private!)
        # noinspection PyProtectedMember
        item = row._asdict()
        return self._load(item)

    def _load(self, item: Dict):
        loaded = {}
        for attr_ in self.meta.attrs:
            if attr_.name not in item:
                continue
            value = item.get(attr_.name)
            if (
                attr_.attr_type == AttrType.JSON
                and not self.engine.dialect.name == POSTGRES
            ):
                value = json.loads(value)
            elif attr_.attr_type == AttrType.DATETIME and value:
                if not value.tzinfo:
                    value.replace(tzinfo=timezone.utc)
                value = value.strftime("%Y-%m-%dT%H:%M:%S+00:00")
            loaded[attr_.name] = value
        # noinspection PyTypeChecker
        result = marshy.load(self.meta.get_read_dataclass(), loaded)
        return result

    def _dump(self, item: T, is_update: bool):
        dumped = {}
        for attr_ in self.meta.attrs:
            value = getattr(item, attr_.name, UNDEFINED)
            if is_update:
                generator = attr_.update_generator
            else:
                generator = attr_.create_generator
            if generator:
                value = generator.transform(value)
            setattr(item, attr_.name, value)
            if value is UNDEFINED:
                continue
            if (
                attr_.attr_type == AttrType.JSON
                and not self.engine.dialect.name == POSTGRES
            ):
                value = json.dumps(value)
            dumped[attr_.name] = _transform_type(value)

        return dumped

    def _key_where_clause(self):
        key_where_clause = None
        for attr_name in self.meta.key_config.get_key_attrs():
            # exp = self.table.columns.get(attr_.name) == f':{attr_.name}'
            exp = self.table.columns.get(attr_name) == BindParameter(attr_name)
            if key_where_clause:
                key_where_clause &= exp
            else:
                key_where_clause = exp
        return key_where_clause

    def _key_where_clause_from_dicts(self, dicts: List[ExternalItemType]):
        key_attrs = list(self.meta.key_config.get_key_attrs())
        if len(key_attrs) == 1:
            keys_for_where = [d.get(key_attrs[0]) for d in dicts]
            where_clause = self.table.columns[key_attrs[0]].in_(keys_for_where)
        else:
            where_clause = []
            for d in dicts:
                where_clause.append(self._key_where_clause_from_dict(d))
            where_clause = or_(*where_clause)
        return where_clause

    def _key_where_clause_from_dict(self, item: ExternalItemType):
        where_clause = and_(
            self.table.columns.get(attr_name) == item.get(attr_name)
            for attr_name in self.meta.key_config.get_key_attrs()
        )
        return where_clause

    def _key_where_clause_from_item(self, item: T):
        where_clause = and_(
            self.table.columns.get(attr_name) == getattr(item, attr_name)
            for attr_name in self.meta.key_config.get_key_attrs()
        )
        return where_clause

    def _key_where_clause_from_items(self, items: List[T]):
        key_attrs = list(self.meta.key_config.get_key_attrs())
        if len(key_attrs) == 1:
            keys_for_where = [getattr(item, key_attrs[0]) for item in items]
            where_clause = self.table.columns[key_attrs[0]].in_(keys_for_where)
        else:
            where_clause = []
            for item in items:
                where_clause.append(self._key_where_clause_from_item(item))
            where_clause = or_(*where_clause)
        return where_clause

    def _search_filter_to_where_clause(
        self, search_filter: SearchFilterABC
    ) -> Tuple[Any, bool]:
        search_filter = search_filter.lock_attrs(self.meta.attrs)
        context = SearchFilterConverterContext()
        return context.convert(search_filter, self.table, self.meta)

    def _search_order_to_order_by(self, search_order: SearchOrder):
        if not search_order:
            return
        search_order.validate_for_attrs(self.meta.attrs)
        orders = []
        for order_attr in search_order.orders:
            order = self.table.columns.get(order_attr.attr)
            if order_attr.desc:
                order = order.desc()
            orders.append(order)
        return orders

    def _default_order_by(self) -> List[Column]:
        orders = [
            self.table.columns.get(attr_name)
            for attr_name in self.meta.key_config.get_key_attrs()
        ]
        return orders

    def _key_as_dict(self, item: T):
        result = self.meta.get_stored_dataclass()()
        required = self.meta.key_config.get_key_attrs()
        for attr in self.meta.attrs:
            if attr.name in required:
                setattr(result, attr.name, getattr(item, attr.name))
        return marshy.dump(result)


def _transform_type(value):
    if isinstance(value, UUID):
        return str(value)
    return value
