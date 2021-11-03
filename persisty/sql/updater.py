from dataclasses import dataclass
from typing import TypeVar, Generic, Type, Iterable

from marshy import get_default_context
from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.types import ExternalItemType

from persisty.errors import PersistyError
from persisty.sql.sql_table import SqlTable

T = TypeVar('T')


@dataclass(frozen=True)
class Updater(Generic[T]):
    _key_col_name: str
    _update_col_names: Iterable[str]
    _sql: str
    _marshaller: MarshallerABC

    def update(self, cursor, item: T):
        dumped: ExternalItemType = self._marshaller.dump(item)
        values = [dumped.get(c) for c in self._update_col_names]
        values.append(dumped.get(self._key_col_name))
        try:
            cursor.execute(self._sql, values)
            if cursor.rowcount == 0:
                raise PersistyError(f'missing_value:{item}')
        except Exception as e:
            raise e if isinstance(e, PersistyError) else PersistyError(str(e))
        return item


def updater(sql_table: SqlTable, item_type: Type[T]):
    key_col_name = sql_table.key_col_name
    update_col_names = tuple(c.name for c in sql_table.cols if c.name != key_col_name)
    return Updater(
        _key_col_name=key_col_name,
        _update_col_names=update_col_names,
        _sql=f"UPDATE {sql_table.name} SET {','.join(f'{c}=?' for c in update_col_names)} WHERE {key_col_name}=?",
        _marshaller=get_default_context().get_marshaller(item_type)
    )
