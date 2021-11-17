from dataclasses import dataclass
from typing import Union, Iterable, Sized, Type, Iterator

from persisty.storage.sql.sql_index import SqlIndex
from persisty.storage.sql.sql_col import SqlCol, cols_for_type


@dataclass(frozen=True)
class SqlTable:
    name: str
    cols: Union[Iterable[SqlCol], Sized]
    key_col_name: str = 'id'
    indexes: [Union[Iterable[SqlIndex], Sized]] = tuple()
    auto_increment: bool = True

    def __post_init__(self):
        assert len({c.name for c in self.cols}) == len(self.cols)
        key_col = next(c for c in self.cols if c.name == self.key_col_name)
        object.__setattr__(self, '_key_col', key_col)
        for index in self.indexes:
            index.validate(self.cols)

    @property
    def key_col(self):
        return getattr(self, '_key_col')

    def create_table_sql(self) -> str:
        return f"""
            CREATE TABLE {self.name} (
                {','.join(c.to_sql(c.name == self.key_col_name, self.auto_increment) for c in self.cols)}
            ) 
        """

    def create_index_sql(self) -> Iterable[str]:
        for index in self.indexes:
            sql = index.to_sql(self.name)
            yield sql


def sql_table_from_type(item_type: Type, key_col_name: str = 'id'):
    sql_table = getattr(item_type, '__sql_table__', None)
    if sql_table:
        return sql_table
    cols = cols_for_type(item_type)
    cols = tuple(_replace_text_primary_key(cols, key_col_name))
    auto_increment = bool(next((c for c in cols if c.name == key_col_name and c.sql_type == 'INT'), False))
    return SqlTable(name=item_type.__name__,
                    cols=cols,
                    key_col_name=key_col_name,
                    auto_increment=auto_increment)


def _replace_text_primary_key(cols: Iterator[SqlCol], key_col_name: str) -> Iterator[SqlCol]:
    """
    Cater for case where id is a completely unannotated string
    """
    for col in cols:
        if col.name == key_col_name and not col.not_null and col.sql_type == 'TEXT':
            yield SqlCol(col.name, True, 'VARCHAR(64)')
        else:
            yield col
