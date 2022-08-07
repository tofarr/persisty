from dataclasses import field, dataclass
from typing import Optional, Dict, List, Iterator

from marshy.types import ExternalItemType
from sqlalchemy import MetaData, Table
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from persisty.context.meta_storage_abc import (
    MetaStorageABC,
    STORED_STORAGE_META,
    STORAGE_META_MARSHALLER,
)
from persisty.impl.sqlalchemy.sqlalchemy_connector import get_default_engine
from persisty.impl.sqlalchemy.sqlalchemy_table_converter import SqlalchemyTableConverter
from persisty.impl.sqlalchemy.sqlalchemy_table_storage import SqlalchemyTableStorage
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.batch_edit import BatchEdit
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta
from persisty.storage.wrapper_storage_abc import WrapperStorageABC


def table_for_storage_meta(engine: Engine, metadata: MetaData) -> Table:
    converter = SqlalchemyTableConverter(engine, metadata)
    table = converter.to_sqlalchemy_table(STORED_STORAGE_META)
    return table


def new_default_meta_storage():
    """Creates a tables storeage with the dialect from the default engine"""
    metadata = MetaData()
    storage = SqlalchemyTableStorage(
        STORED_STORAGE_META, table_for_storage_meta(get_default_engine(), metadata)
    )
    return storage


@dataclass
class SqlalchemyMetaStorage(MetaStorageABC, WrapperStorageABC):
    """
    Sqlalchemy storage which holds storage_meta in a table in sql.
    """

    meta_storage: SqlalchemyTableStorage = field(
        default_factory=new_default_meta_storage
    )
    schema: Dict[str, StorageMeta] = None
    storage: Dict[str, SqlalchemyTableStorage] = field(default_factory=dict)

    def __post_init__(self):
        """If no schema was specified, we try to load details of existing stores from the database"""
        if self.schema is not None:
            return
        with self.meta_storage.session_maker.begin() as session:
            self.meta_storage.table.create(bind=session, checkfirst=True)
            # for storage_meta in self.meta_storage.search_all():

    def storage_from_meta(self, storage_meta: StorageMeta):
        converter = SqlalchemyTableConverter(
            self.meta_storage.table.metadata, self.schema
        )
        table = converter.to_sqlalchemy_table(storage_meta)
        storage = SqlalchemyTableStorage(
            storage_meta, table, self.meta_storage.session_maker
        )
        return storage

    def get_item_storage(self, name: str) -> Optional[StorageABC]:
        storage = self.item_storage.get(name)
        if storage:
            return storage
        storage_meta = self.meta_storage.read(name)
        if storage_meta:
            storage_meta = self.storage_meta_marshaller.load(storage_meta)
            storage = self.storage_from_meta(storage_meta)
            self.item_storage[storage_meta.name] = storage
            return storage

    def get_storage(self) -> StorageABC:
        return self.meta_storage

    def update(
        self, updates: ExternalItemType, search_filter: SearchFilterABC = INCLUDE_ALL
    ) -> Optional[ExternalItemType]:
        new_item = self.get_storage().update(updates, search_filter)
        return new_item

    def after_update(self, new_item: ExternalItemType):
        if new_item:
            storage_meta = self.storage_meta_marshaller.load(new_item)
            storage = self.item_storage.get(storage_meta.name)
            if storage:
                storage = _unnest(storage)
                storage.storage_meta = storage_meta

    def delete(self, key: str) -> bool:
        result = self.get_storage().delete(key)
        if result:
            self.after_delete(key)
        return result

    def after_delete(self, key: str):
        storage = self.item_storage.pop(key, None)
        if storage:
            storage = _unnest(storage)
            storage.items = None  # Trash the storage to prevent future use

    def edit_batch(self, edits: List[BatchEdit]):
        results = self.get_storage().edit_batch(edits)
        for result in results:
            self.after_batch_edit(result)
        return results

    def edit_all(self, edits: Iterator[BatchEdit]) -> Iterator[BatchEditResult]:
        for result in self.get_storage().edit_all(edits):
            self.after_batch_edit(result)
            yield result

    def after_batch_edit(self, result: BatchEditResult):
        if result.success:
            if result.edit.update_item:
                self.after_update(result.edit.update_item)
            elif result.edit.delete_key:
                self.after_delete(result.edit.delete_key)
