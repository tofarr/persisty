from dataclasses import dataclass
from typing import Union, Iterable, Sized

from persisty.storage.sql.sql_col import SqlCol


@dataclass(frozen=True)
class SqlIndex:
    col_named: Union[Iterable[str], Sized]

    def validate(self, col_named: Union[Iterable[SqlCol], Sized]):
        for col in self.col_named:
            next(c for c in col_named if c.name == col)
