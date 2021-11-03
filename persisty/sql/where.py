import itertools
from dataclasses import dataclass
from typing import Any, Union, Iterable, Sized, Optional

from persisty.item_filter import AttrFilter, AttrFilterOp
from persisty.item_filter.and_filter import AndFilter
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.item_filter.not_filter import NotFilter
from persisty.item_filter.or_filter import OrFilter
from persisty.item_filter.query_filter import QueryFilter
from persisty.sql.sql_table import SqlTable


@dataclass(frozen=True)
class Where:
    sql: str
    params: Union[Iterable[Any], Sized]
    pure_sql: bool


def build_where(item_filter: Optional[ItemFilterABC], sql_table: SqlTable):
    if item_filter is None:
        return Where('', [], True)
    type_: Any = item_filter.__class__
    type_builder = TYPE_BUILDERS.get(type_)
    if type_builder is None:
        return Where('', tuple(), False)
    # noinspection PyTypeChecker
    where = type_builder(item_filter, sql_table)
    return where


def _filter_and(item_filter: AndFilter, sql_table: SqlTable):
    wheres = [build_where(f, sql_table) for f in item_filter.filters]
    where = Where(
        sql=f"({' AND '.join(w.sql for w in wheres if w.sql)})",
        params=tuple(itertools.chain(*(w.params for w in wheres))),
        pure_sql=next((w.pure_sql for w in wheres if w.pure_sql is False), False)
    )
    return where


def _filter_or(item_filter: OrFilter, sql_table: SqlTable):
    wheres = [build_where(f, sql_table) for f in item_filter.filters]
    where = Where(
        sql=f"({' OR '.join(w.sql for w in wheres if w.sql)})",
        params=tuple(itertools.chain(*(w.params for w in wheres))),
        pure_sql=next((w.pure_sql for w in wheres if w.pure_sql is False), False)
    )
    return where


def _filter_not(item_filter: NotFilter, sql_table: SqlTable):
    where = build_where(item_filter.filter, sql_table)
    where = Where(f'NOT {where.sql}', where.params, where.pure_sql)
    return where


def _filter_query(item_filter: QueryFilter, sql_table: SqlTable):
    columns = [c.name for c in sql_table.cols if not c.name.endswith('_id') and c.external_type == str]
    if not columns:
        return Where('', tuple(), False)
    # noinspection PyUnusedLocal
    return Where(
        sql=f"({' OR '.join(f'{c.name} like ?' for c in columns)})",
        params=[item_filter.query for c in columns],
        pure_sql=True
    )


# noinspection DuplicatedCode
def _filter_attr(item_filter: AttrFilter, sql_table: SqlTable):
    attr = item_filter.attr
    col = next((c for c in sql_table.cols if c.name == attr), None)
    if not col:
        return Where('', tuple(), False)
    op = item_filter.op
    op_fn = OP_BUILDERS[op]
    if op_fn:
        return op_fn(attr, item_filter.value)
    if not col:
        return Where('', tuple(), False)


TYPE_BUILDERS = {
    AndFilter: _filter_and,
    OrFilter: _filter_or,
    NotFilter: _filter_not,
    AttrFilter: _filter_attr,
    QueryFilter: _filter_query,
}


def _attr_contains(attr: str, value: str):
    return Where(f'{attr} like ?', [f'%{value}%'], True)


def _attr_endswith(attr: str, value: str):
    return Where(f'{attr} like ?', [f'%{value}'], True)


def _attr_eq(attr: str, value: str):
    return Where(f"{attr} = ?", [value], True)


def _attr_gt(attr: str, value: str):
    return Where(f"{attr} > ?", [value], True)


def _attr_gte(attr: str, value: str):
    return Where(f"{attr} >= ?", [value], True)


def _attr_lt(attr: str, value: str):
    return Where(f"{attr} < ?", [value], True)


def _attr_lte(attr: str, value: str):
    return Where(f"{attr} <= ?", [value], True)


def _attr_ne(attr: str, value: str):
    return Where(f"{attr} <> ?", [value], True)


def _attr_oneof(attr: str, value: str):
    # noinspection PyUnusedLocal
    return Where(f"{attr} in ({','.join('?' for v in value)})", value, True)


def _attr_startswith(attr: str, value: str):
    return Where(f'{attr} like ?', [f'{value}%'], True)


OP_BUILDERS = {
    AttrFilterOp.contains: _attr_contains,
    AttrFilterOp.endswith: _attr_endswith,
    AttrFilterOp.eq: _attr_eq,
    AttrFilterOp.gt: _attr_gt,
    AttrFilterOp.gte: _attr_gte,
    AttrFilterOp.lt: _attr_lt,
    AttrFilterOp.lte: _attr_lte,
    AttrFilterOp.ne: _attr_ne,
    AttrFilterOp.oneof: _attr_oneof,
    AttrFilterOp.startswith: _attr_startswith,
}
