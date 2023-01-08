from dataclasses import field, dataclass
from sqlalchemy import MetaData, Table
from sqlalchemy.engine import Engine

from persisty.impl.sqlalchemy.sqlalchemy_table_converter import SqlalchemyTableConverter
from persisty.storage.storage_meta import StorageMeta


@dataclass
class SqlalchemyContext:
    engine: Engine
    developer_mode: bool = False
    meta_data: MetaData = field(default_factory=MetaData)

    @property
    def converter(self) -> SqlalchemyTableConverter:
        converter = getattr(self, '_converter', None)
        if not converter:
            converter = SqlalchemyTableConverter(self.engine, self.meta_data)
            setattr(self, '_converter', converter)
        return converter

    def get_table(self, storage_meta: StorageMeta) -> Table:
        tables_by_storage_name = getattr(self, '_tables_by_storage_name', None)
        if not tables_by_storage_name:
            tables_by_storage_name = {}
            setattr(self, '_tables_by_storage_name', tables_by_storage_name)
        table = tables_by_storage_name.get(storage_meta.name)
        if table is None:
            table = self.converter.to_sqlalchemy_table(storage_meta)
            tables_by_storage_name[storage_meta.name] = table
            if self.developer_mode:
                table.create(self.engine)
        return table
