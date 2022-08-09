import json
from datetime import datetime
from typing import Optional, List, Iterator, Tuple, Any, Dict

from dataclasses import dataclass, field
from uuid import UUID

from marshy.types import ExternalItemType
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DatabaseError
from sqlalchemy.sql.elements import BindParameter, or_

from persisty.access_control.constants import ALL_ACCESS
from persisty.errors import PersistyError
from persisty.field.field import Field
from persisty.field.field_type import FieldType
from persisty.impl.sqlalchemy.search_filter.search_filter_converter_context import (
    SearchFilterConverterContext,
)
from persisty.impl.sqlalchemy.sqlalchemy_column_converter import POSTGRES
from persisty.impl.sqlalchemy.sqlalchemy_connector import get_default_engine
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.storage.batch_edit import BatchEdit
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.storage.schema_validating_storage import SchemaValidatingStorage
from persisty.storage.secured_storage import SecuredStorage
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta

from sqlalchemy import Table, and_, select, func, Column

from persisty.util import UNDEFINED, from_base64, to_base64


def sqlalchemy_table_storage(
    storage_meta: StorageMeta,
    table: Table,
    engine: Optional[Engine] = None,
):
    if engine is None:
        engine = get_default_engine()
    storage = SqlalchemyTableStorage(storage_meta, table, engine)
    if storage_meta.access_control is not ALL_ACCESS:
        storage = SecuredStorage(storage)
    storage = SchemaValidatingStorage(storage)
    return storage


def catch_db_error(fn):
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except DatabaseError as e:
            raise PersistyError(e)

    return wrapper


@dataclass(frozen=True)
class SqlalchemyTableStorage(StorageABC):
    """
    This class uses sql alchemy at a lower level than the standard orm usage
    """

    storage_meta: StorageMeta
    table: Table
    engine: Engine = field(default_factory=get_default_engine)

    def get_storage_meta(self) -> StorageMeta:
        return self.storage_meta

    @catch_db_error
    def create(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        dumped = self._dump(item, False)
        with self.engine.begin() as connection:
            result = connection.execute(self.table.insert(), parameters=dumped)
            if result.inserted_primary_key:
                item.update(self._load_row(result.inserted_primary_key))
            connection.commit()
            return item

    @catch_db_error
    def read(self, key: str) -> Optional[ExternalItemType]:
        with self.engine.begin() as connection:
            return self._read(connection, self._key_from_str(key))

    def _read(self, connection, key: Dict) -> Optional[Dict]:
        stmt = self.table.select(whereclause=self._key_where_clause())
        row = connection.execute(stmt, key).first()
        if row:
            item = self._load_row(row)
            return item

    def read_batch(self, keys: List[str]) -> List[Optional[ExternalItemType]]:
        with self.engine.begin() as connection:
            key_config = self.storage_meta.key_config
            key_dicts = [key_config.from_key_str(k) for k in keys]
            items = self._read_batch(connection, key_dicts)
            items_by_key = {key_config.to_key_str(item): item for item in items}
            items = [items_by_key.get(k) for k in keys]
            return items

    def _read_batch(
        self, connection, keys: List[Dict], cols: Optional[List[Column]] = None
    ) -> List[Dict]:
        # NB: Does not enforce ordering
        assert len(keys) <= self.storage_meta.batch_size
        where_clause = self._key_where_clause_from_items(keys)
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
        item: ExternalItemType,
        updates: ExternalItemType,
        search_filter: SearchFilterABC = INCLUDE_ALL,
    ) -> Optional[ExternalItemType]:
        with self.engine.begin() as connection:
            updates = self._dump(updates, True)
            stmt = self.table.update(self._key_where_clause_from_item(updates))
            connection.execute(stmt, updates)
            key = self.storage_meta.key_config.from_key_str(key)
            loaded = self._read(connection, key)
            connection.commit()
            return loaded

    @catch_db_error
    def delete(self, key: str) -> bool:
        with self.engine.begin() as connection:
            key = self.storage_meta.key_config.from_key_str(key)
            stmt = self.table.delete(whereclause=self._key_where_clause())
            result = connection.execute(stmt, key)
            connection.commit()
            return bool(result.rowcount)

    @catch_db_error
    def _delete(self, key: str, item: ExternalItemType) -> bool:
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
    ) -> ResultSet[ExternalItemType]:
        assert limit <= self.storage_meta.batch_size
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
                    self.table.columns.get(f.name) > page_key[1][f.name]
                    for f in self._key_fields()
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
                if handled or search_filter.match(result, self.storage_meta.fields):
                    results.append(result)
                    if len(results) == limit:
                        if search_order and search_order.orders:
                            next_page_key = ["offset", offset + len(results)]
                        if not search_order or not search_order.orders:
                            next_page_key = [
                                "id",
                                {f.name: result[f.name] for f in self._key_fields()},
                            ]
                        next_page_key = to_base64(next_page_key)
                        break
            return ResultSet(results, next_page_key)

    @catch_db_error
    def search_all(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
    ) -> Iterator[ExternalItemType]:
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
                if not handled and not search_filter.match(
                    item, self.storage_meta.fields
                ):
                    continue
                yield item

    @catch_db_error
    def edit_batch(self, edits: List[BatchEdit]) -> List[BatchEditResult]:
        assert len(edits) <= self.storage_meta.batch_size
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
        key_fields = [f.name for f in self._key_fields()]
        to_key_str = self.storage_meta.key_config.to_key_str
        for edit in edits:
            edit_key = to_key_str(edit.update_item)
            edits_by_key[edit_key] = edit
            key_dicts_to_load_row.append({k: edit.update_item[k] for k in key_fields})
            results_by_id[edit.id] = BatchEditResult(edit)
        existing_items = self._read_batch(connection, key_dicts_to_load_row)
        for item in existing_items:
            key = to_key_str(item)
            edit = edits_by_key[key]
            item.update(**edit.update_item)
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
        key_config = self.storage_meta.key_config
        delete_keys = [key_config.from_key_str(d.delete_key) for d in edits]
        existing_keys = self._get_existing_keys(connection, delete_keys)
        where_clause = self._key_where_clause_from_items(delete_keys)
        stmt = self.table.delete(whereclause=where_clause)
        connection.execute(stmt)
        deleted_keys = {key_config.to_key_str(k) for k in existing_keys}
        for delete in edits:
            deleted = delete.delete_key in deleted_keys
            results_by_id[delete.id] = BatchEditResult(delete, deleted)

    def _get_existing_keys(self, connection, keys: List[Dict]) -> List[Dict]:
        key_cols = [self.table.columns[k.name] for k in self._key_fields()]
        existing_keys = self._read_batch(connection, keys, key_cols)
        return existing_keys

    def _load_row(self, row):
        # Row is a KeyedTuple - _asdict is to match the namedtuple API (it's not private!)
        # noinspection PyProtectedMember
        item = row._asdict()
        return self._load(item)

    def _load(self, item: Dict):
        loaded = {}
        for field_ in self.storage_meta.fields:
            if field_.name not in item:
                continue
            value = item.get(field_.name)
            if (
                field_.type == FieldType.JSON
                and not self.engine.dialect.name == POSTGRES
            ):
                value = json.loads(value)
            elif field_.type == FieldType.DATETIME and value:
                value = value.isoformat()
            loaded[field_.name] = value
        return loaded

    def _dump(self, item: ExternalItemType, is_update: bool):
        dumped = {}
        for field_ in self.storage_meta.fields:
            value = item.get(field_.name, UNDEFINED)
            if field_.write_transform:
                value = field_.write_transform.transform(value, is_update)
                item[field_.name] = value
            if value is UNDEFINED:
                continue
            if (
                field_.type == FieldType.JSON
                and not self.engine.dialect.name == POSTGRES
            ):
                value = json.dumps(value)
            if field_.type == FieldType.DATETIME and value:
                value = datetime.fromisoformat(value)
            dumped[field_.name] = value

        return dumped

    def _key_fields(self) -> Iterator[Field]:
        for field_ in self.storage_meta.fields:
            if self.storage_meta.key_config.is_required_field(field_.name):
                yield field_

    def _non_key_fields(self) -> Iterator[Field]:
        for field_ in self.storage_meta.fields:
            if not self.storage_meta.key_config.is_required_field(field_.name):
                yield field_

    def _key_where_clause(self):
        key_where_clause = None
        for field_ in self._key_fields():
            # exp = self.table.columns.get(field_.name) == f':{field_.name}'
            exp = self.table.columns.get(field_.name) == BindParameter(field_.name)
            if key_where_clause:
                key_where_clause &= exp
            else:
                key_where_clause = exp
        return key_where_clause

    def _key_from_str(self, key: str):
        return self.storage_meta.key_config.from_key_str(key)

    def _key_where_clause_from_item(self, item: ExternalItemType):
        where_clause = and_(
            self.table.columns.get(field_.name) == item[field_.name]
            for field_ in self._key_fields()
        )
        return where_clause

    def _key_where_clause_from_items(self, items: List[ExternalItemType]):
        key_fields = list(self._key_fields())
        if len(key_fields) == 1:
            keys_for_where = [item[key_fields[0].name] for item in items]
            where_clause = self.table.columns[key_fields[0].name].in_(keys_for_where)
        else:
            where_clause = []
            for item in items:
                where_clause.append(self._key_where_clause_from_item(item))
            where_clause = or_(*where_clause)
        return where_clause

    def _search_filter_to_where_clause(
        self, search_filter: SearchFilterABC
    ) -> Tuple[Any, bool]:
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