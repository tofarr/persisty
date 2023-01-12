from dataclasses import dataclass, field
from sqlalchemy import MetaData, Table
from sqlalchemy.engine import Engine

from persisty.impl.sqlalchemy.sqlalchemy_table_converter import SqlalchemyTableConverter
from persisty.store_meta import StoreMeta


@dataclass
class SqlalchemyContext:
    engine: Engine
    developer_mode: bool = False
    meta_data: MetaData = field(default_factory=MetaData)

    @property
    def converter(self) -> SqlalchemyTableConverter:
        converter = getattr(self, "_converter", None)
        if not converter:
            converter = SqlalchemyTableConverter(self.engine, self.meta_data)
            setattr(self, "_converter", converter)
        return converter

    def get_table(self, store_meta: StoreMeta) -> Table:
        tables_by_store_name = getattr(self, "_tables_by_store_name", None)
        if not tables_by_store_name:
            tables_by_store_name = {}
            setattr(self, "_tables_by_store_name", tables_by_store_name)
        table = tables_by_store_name.get(store_meta.name)
        if table is None:
            table, indexes = self.converter.to_sqlalchemy_table_and_indexes(store_meta)
            tables_by_store_name[store_meta.name] = table
            if self.developer_mode:
                table.create(self.engine)
                for index in indexes:
                    index.create(self.engine)
        return table
