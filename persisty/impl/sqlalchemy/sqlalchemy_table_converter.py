import base64
import hashlib
import os
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Tuple

from sqlalchemy import Table, MetaData, Index
from sqlalchemy.engine import Engine

from persisty.impl.sqlalchemy.sqlalchemy_column_converter import (
    SqlalchemyColumnConverter,
)
from persisty.impl.sqlalchemy.sqlalchemy_constraint_converter import (
    SqlalchemyConstraintConverter,
)
from persisty.index.attr_index import AttrIndex
from persisty.index.unique_index import UniqueIndex
from persisty.store_meta import StoreMeta
from persisty.util import secure_hash


def is_using_sql_native_constraints():
    return os.environ.get("PERSISTY_SQL_NATIVE_CONSTRAINTS") != "0"


@dataclass
class SqlalchemyTableConverter:
    """Converter for store meta to / from sqlalchemy tables"""

    engine: Engine
    metadata: MetaData
    schema: Dict[str, StoreMeta] = field(default_factory=dict)
    native_constraints: bool = field(default_factory=is_using_sql_native_constraints)

    # pylint: disable=R0914
    def to_sqlalchemy_table_and_indexes(
        self, store_meta: StoreMeta
    ) -> Tuple[Table, List[Index]]:
        args = [
            store_meta.name,
            self.metadata,
        ]
        dialect = self.engine.dialect.name
        column_factory = SqlalchemyColumnConverter(store_meta.key_config, dialect)
        columns_by_name = {}
        indexes = []
        for attr_ in store_meta.attrs:
            column = column_factory.create_column(attr_)
            columns_by_name[attr_.name] = column
            args.append(column)
        for index in store_meta.indexes:
            if isinstance(index, AttrIndex):
                name = f"idx__{store_meta.name}__{index.attr_name}"
                if len(name) > 60:
                    name = f"idx_{secure_hash(name).replace('+', '').replace('=', '')}"
                index_cols = [columns_by_name[index.attr_name]]
                unique = False
            elif isinstance(index, UniqueIndex):
                name = f"idx__{store_meta.name}__{'__'.join(index.attr_names)}"
                if len(name) > 60:
                    name = f"idx_{secure_hash(name).replace('+', '').replace('=', '')}"
                index_cols = [columns_by_name[a] for a in index.attr_names]
                unique = True
            else:
                continue
            if len(name) > 62:
                name = base64.b64decode(hashlib.md5(name.encode("UTF-8"))).decode(
                    "UTF-8"
                )
            index_obj = Index(name, *index_cols, unique=unique)
            indexes.append(index_obj)

        if self.native_constraints:
            constraint_factory = SqlalchemyConstraintConverter(self.schema)
            args.extend(constraint_factory.get_foreign_key_constraints(store_meta))
        table = Table(*args)
        return table, indexes

    def to_sqlalchemy_tables_and_indexes(self) -> Iterator[Tuple[Table, List[Index]]]:
        for store_meta in self.schema.values():
            table_and_indexes = self.to_sqlalchemy_table_and_indexes(store_meta)
            yield table_and_indexes
