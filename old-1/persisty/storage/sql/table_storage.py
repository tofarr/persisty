from dataclasses import dataclass
from typing import Optional, Iterator, Type, Callable, Any

from persisty.item_filter import AttrFilter, AttrFilterOp
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.page import Page

from persisty.storage.sql.destroyer import Destroyer, destroyer
from persisty.storage.sql.inserter import Inserter, inserter
from persisty.storage.sql.searcher import Searcher, table_searcher
from persisty.storage.sql.sql_table import SqlTable, sql_table_from_type
from persisty.storage.sql.updater import Updater, updater
from persisty.storage.storage_abc import StorageABC, T
from persisty.storage.storage_filter import StorageFilter
from persisty.storage.storage_meta import StorageMeta, storage_meta_from_dataclass


@dataclass(frozen=True)
class TableStorage(StorageABC[T]):
    get_cursor: Callable[[], Any]
    item_meta: StorageMeta
    sql_table: SqlTable
    inserter: Inserter[T]
    updater: Updater[T]
    destroyer: Destroyer
    searcher: Searcher[T]

    @property
    def meta(self) -> StorageMeta:
        return self.item_meta

    @property
    def item_type(self) -> Type[T]:
        return self.searcher.type

    def create(self, item: T) -> str:
        with self.get_cursor() as cursor:
            return self.inserter.insert(cursor, item)

    def read(self, key: str) -> Optional[T]:
        with self.get_cursor() as cursor:
            storage_filter = StorageFilter(AttrFilter(self.sql_table.key_col_name, AttrFilterOp.eq, key))
            items = self.searcher.search(cursor, storage_filter)
            item = next(items, None)
            return item

    def update(self, item: T) -> T:
        with self.get_cursor() as cursor:
            return self.updater.update(cursor, item)

    def destroy(self, key: str) -> bool:
        with self.get_cursor() as cursor:
            return self.destroyer.destroy(cursor, key)

    def search(self, storage_filter: Optional[StorageFilter[T]] = None) -> Iterator[T]:
        with self.get_cursor() as cursor:
            yield from self.searcher.search(cursor, storage_filter)

    def count(self, item_filter: Optional[ItemFilterABC[T]] = None) -> int:
        with self.get_cursor() as cursor:
            count = self.searcher.count(cursor, item_filter)
            return count

    def paged_search(self, storage_filter: Optional[StorageFilter[T]] = None, page_key: Optional[str] = None,
                     limit: int = 20) -> Page[T]:
        with self.get_cursor() as cursor:
            page = self.searcher.paged_search(cursor, storage_filter, page_key, limit)
            return page


def table_storage(get_cursor: Callable[[], Any],
                  item_type: Optional[Type[T]] = None,
                  storage_meta: Optional[StorageMeta] = None):
    if storage_meta is None:
        storage_meta = storage_meta_from_dataclass(item_type)
    if item_type is None:
        item_type = storage_meta.to_dataclass()
    sql_table = sql_table_from_type(item_type)
    return TableStorage(get_cursor=get_cursor,
                        item_meta=storage_meta,
                        sql_table=sql_table,
                        inserter=inserter(sql_table, item_type),
                        updater=updater(sql_table, item_type),
                        destroyer=destroyer(sql_table),
                        searcher=table_searcher(sql_table, item_type))
