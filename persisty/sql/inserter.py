from dataclasses import dataclass
from typing import TypeVar, Generic, Type, Iterable, Callable
from uuid import uuid4

from marshy import get_default_context
from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.types import ExternalItemType

from persisty.errors import PersistyError
from persisty.sql.sql_table import SqlTable

T = TypeVar('T')


@dataclass(frozen=True)
class Inserter(Generic[T]):
    _key_col_name: str
    _insert_col_names: Iterable[str]
    _sql: str
    _marshaller: MarshallerABC
    _key_setter: Callable[[T, str], None]

    def insert(self, cursor, item: T) -> str:
        dumped: ExternalItemType = self._marshaller.dump(item)
        values = []
        for c in self._insert_col_names:
            value = dumped.get(c)
            if c == self._key_col_name and value is None:
                value = str(uuid4())
                if self._key_setter:
                    dumped[self._key_col_name] = value
            values.append(value)
        try:
            cursor.execute(self._sql, values)
            if self._key_col_name:
                key = dumped.get(self._key_col_name)
            else:
                key = str(cursor.lastrowid)
            if self._key_setter:
                self._key_setter(item, key)
            return key
        except Exception as e:
            raise PersistyError(str(e))


def inserter(sql_table: SqlTable, item_type: Type[T]):
    key_col_name = sql_table.key_col_name
    insert_cols = sql_table.cols
    if sql_table.auto_increment:
        insert_cols = tuple(c for c in sql_table.cols if c.name != key_col_name)
    insert_col_names = tuple(c.name for c in insert_cols)
    keys_sql = ','.join(insert_col_names)
    # noinspection PyUnusedLocal
    values_sql = ','.join('?' for c in insert_cols)
    key_setter = lambda item, key: setattr(item, key_col_name, key)
    return Inserter(
        _key_col_name=key_col_name,
        _insert_col_names=insert_col_names,
        _sql=f"INSERT INTO {sql_table.name} ({keys_sql}) VALUES ({values_sql})",
        _marshaller=get_default_context().get_marshaller(item_type),
        _key_setter=key_setter
    )
