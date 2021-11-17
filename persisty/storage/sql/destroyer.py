from dataclasses import dataclass

from old.persisty import PersistyError
from persisty.storage.sql.sql_table import SqlTable


@dataclass(frozen=True)
class Destroyer:
    _sql: str

    def destroy(self, cursor, key: str) -> bool:
        try:
            cursor.execute(self._sql, (key,))
            destroyed = cursor.rowcount != 0
            return destroyed
        except Exception as e:
            raise PersistyError(str(e))


def destroyer(sql_table: SqlTable) -> Destroyer:
    return Destroyer(f"DELETE FROM {sql_table.name} WHERE {sql_table.key_col_name}=?")
