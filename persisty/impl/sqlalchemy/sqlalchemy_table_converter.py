import base64
import hashlib
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
from persisty.store_meta import StoreMeta


@dataclass
class SqlalchemyTableConverter:
    """Converter for storemeta to / from sqlalchemy tables"""

    engine: Engine
    metadata: MetaData
    schema: Dict[str, StoreMeta] = field(default_factory=dict)

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
            name = f"idx_{'__'.join(index.attr_names)}"
            if len(name) > 62:
                name = base64.b64decode(hashlib.md5(name.encode("UTF-8"))).decode(
                    "UTF-8"
                )
            index_cols = [columns_by_name[a] for a in index.attr_names]
            index_obj = Index(name, *index_cols, unique=index.unique)
            indexes.append(index_obj)

        constraint_factory = SqlalchemyConstraintConverter(self.schema)
        args.extend(constraint_factory.get_foreign_key_constraints(store_meta))
        table = Table(*args)
        return table, indexes

    def to_sqlalchemy_tables_and_indexes(self) -> Iterator[Tuple[Table, List[Index]]]:
        for store_meta in self.schema.values():
            table_and_indexes = self.to_sqlalchemy_table_and_indexes(store_meta)
            yield table_and_indexes
