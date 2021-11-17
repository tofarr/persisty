from dataclasses import dataclass
from typing import Optional, Iterator, ForwardRef, Type, Callable, Any

from persisty.cache_header import CacheHeader
from old.persisty.capabilities import Capabilities, ALL_CAPABILITIES
from persisty.item_filter import AttrFilter, AttrFilterOp
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.page import Page
from old.persisty2.storage_filter import StorageFilter

from persisty.storage.sql.destroyer import Destroyer, destroyer
from persisty.storage.sql.inserter import Inserter, inserter
from persisty.storage.sql.searcher import Searcher, searcher
from persisty.storage.sql.sql_table import SqlTable, sql_table_from_type
from persisty.storage.sql.updater import Updater, updater
from old.persisty.storage.storage_abc import StorageABC, T
from old.persisty.storage_schemas import StorageSchemas, schemas_for_type
from persisty.util import secure_hash


@dataclass(frozen=True)
class TableStorage(StorageABC[T]):
    get_cursor: Callable[[], Any]
    _name: str
    _schemas: StorageSchemas[T]
    sql_table: SqlTable
    inserter: Inserter[T]
    updater: Updater[T]
    destroyer: Destroyer
    searcher: Searcher[T]

    @property
    def name(self) -> str:
        return self._name

    @property
    def item_type(self) -> Type[T]:
        return self.searcher.type

    @property
    def capabilities(self) -> Capabilities:
        return ALL_CAPABILITIES

    @property
    def schemas(self) -> ForwardRef('StorageSchemas[T]'):
        return self._schemas

    def get_key(self, item: T) -> str:
        # Assumes item has key attr with correct name
        return getattr(item, self.sql_table.key_col.name)

    def get_cache_header(self, item: T) -> CacheHeader:
        dumped = self.searcher.marshaller.dump(item)
        cache_key = secure_hash(dumped)
        return CacheHeader(cache_key)

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
        cursor = self.get_cursor()
        try:
            items = self.searcher.search(cursor, storage_filter)
            for item in items:
                yield item
        finally:
            self.get_cursor()

    def count(self, item_filter: Optional[ItemFilterABC[T]] = None) -> int:
        with self.get_cursor() as cursor:
            count = self.searcher.count(cursor, item_filter)
            return count

    def paged_search(self, storage_filter: Optional[StorageFilter[T]] = None, page_key: Optional[str] = None,
                     limit: int = 20) -> Page[T]:
        with self.get_cursor() as cursor:
            page = self.searcher.paged_search(cursor, storage_filter, page_key, limit)
            return page


def table_storage(get_cursor: Callable[[], Any], item_type: Type[T], sql_table: Optional[SqlTable] = None):
    if sql_table is None:
        sql_table = sql_table_from_type(item_type)
    return TableStorage(get_cursor=get_cursor,
                      _name=sql_table.name,
                      _schemas=schemas_for_type(item_type),
                      sql_table=sql_table,
                      inserter=inserter(sql_table, item_type),
                      updater=updater(sql_table, item_type),
                      destroyer=destroyer(sql_table),
                      searcher=searcher(sql_table, item_type))

