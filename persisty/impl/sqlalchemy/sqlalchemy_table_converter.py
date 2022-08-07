from dataclasses import dataclass, field
from typing import Dict, Iterator

from sqlalchemy import Table, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from persisty.impl.sqlalchemy.sqlalchemy_column_converter import (
    SqlalchemyColumnConverter,
)
from persisty.impl.sqlalchemy.sqlalchemy_constraint_converter import (
    SqlalchemyConstraintConverter,
)
from persisty.storage.storage_meta import StorageMeta


@dataclass
class SqlalchemyTableConverter:
    """Converter for storagemeta to / from sqlalchemy tables"""

    engine: Engine
    metadata: MetaData
    schema: Dict[str, StorageMeta] = field(default_factory=dict)

    def to_sqlalchemy_table(self, storage_meta: StorageMeta) -> Table:
        args = [
            storage_meta.name,
            self.metadata,
        ]
        dialect = self.engine.dialect.name
        column_factory = SqlalchemyColumnConverter(storage_meta.key_config, dialect)
        for field_ in storage_meta.fields:
            args.append(column_factory.create_column(field_))
        constraint_factory = SqlalchemyConstraintConverter(self.schema)
        args.extend(constraint_factory.get_foreign_key_constraints(storage_meta))
        table = Table(*args)
        return table

    def to_sqlalchemy_tables(self) -> Iterator[Table]:
        for storage_meta in self.schema.values():
            table = self.to_sqlalchemy_table(storage_meta)
            yield table
