from dataclasses import dataclass
from typing import Optional, Iterator, ForwardRef, Type, Callable, Any

from persisty.capabilities import Capabilities, ALL_CAPABILITIES
from persisty.item_filter import AttrFilter, AttrFilterOp
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.page import Page
from persisty.search_filter import SearchFilter

from persisty.sql.destroyer import Destroyer, destroyer
from persisty.sql.inserter import Inserter, inserter
from persisty.sql.searcher import Searcher, searcher
from persisty.sql.sql_table import SqlTable, sql_table_from_type
from persisty.sql.updater import Updater, updater
from persisty.store.store_abc import StoreABC, T
from persisty.store_schemas import StoreSchemas, schemas_for_type


@dataclass(frozen=True)
class TableStore(StoreABC[T]):
    get_cursor: Callable[[], Any]
    _name: str
    _schemas: StoreSchemas[T]
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
    def schemas(self) -> ForwardRef('StoreSchemas[T]'):
        return self._schemas

    def get_key(self, item: T) -> str:
        # Assumes item has key attr with correct name
        return getattr(item, self.sql_table.key_col.name)

    def create(self, item: T) -> str:
        with self.get_cursor() as cursor:
            return self.inserter.insert(cursor, item)

    def read(self, key: str) -> Optional[T]:
        with self.get_cursor() as cursor:
            search_filter = SearchFilter(AttrFilter(self.sql_table.key_col_name, AttrFilterOp.eq, key))
            items = self.searcher.search(cursor, search_filter)
            item = next(items, None)
            return item

    def update(self, item: T) -> T:
        with self.get_cursor() as cursor:
            return self.updater.update(cursor, item)

    def destroy(self, key: str) -> bool:
        with self.get_cursor() as cursor:
            return self.destroyer.destroy(cursor, key)

    def search(self, search_filter: Optional[SearchFilter[T]] = None) -> Iterator[T]:
        cursor = self.get_cursor()
        try:
            items = self.searcher.search(cursor, search_filter)
            for item in items:
                yield item
        finally:
            self.get_cursor()

    def count(self, item_filter: Optional[ItemFilterABC[T]] = None) -> int:
        with self.get_cursor() as cursor:
            count = self.searcher.count(cursor, item_filter)
            return count

    def paged_search(self, search_filter: Optional[SearchFilter[T]] = None, page_key: Optional[str] = None,
                     limit: int = 20) -> Page[T]:
        with self.get_cursor() as cursor:
            page = self.searcher.paged_search(cursor, search_filter, page_key, limit)
            return page


def table_store(get_cursor: Callable[[], Any], item_type: Type[T], sql_table: Optional[SqlTable] = None):
    if sql_table is None:
        sql_table = sql_table_from_type(item_type)
    return TableStore(get_cursor=get_cursor,
                      _name=sql_table.name,
                      _schemas=schemas_for_type(item_type),
                      sql_table=sql_table,
                      inserter=inserter(sql_table, item_type),
                      updater=updater(sql_table, item_type),
                      destroyer=destroyer(sql_table),
                      searcher=searcher(sql_table, item_type))

