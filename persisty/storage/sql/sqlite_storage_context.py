from dataclasses import dataclass, field
import re
from sqlite3 import OperationalError
from typing import Optional, Iterator, Callable, Any, Dict

from persisty.access_control.access_control import AccessControl
from persisty.access_control.access_control_abc import AccessControlABC
from persisty.attr.attr_mode import AttrMode
from persisty.errors import PersistyError
from persisty.item_filter import AttrFilter, AttrFilterOp
from persisty.item_filter.and_filter import AndFilter
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.key_config.multi_key_config import MultiKeyConfig
from persisty.page import Page
from persisty.storage.sql.searcher import table_searcher
from persisty.storage.sql.sql_col import SqlCol
from persisty.storage.sql.sql_table import SqlTable, sql_table_from_type
from persisty.storage.sql.table_storage import table_storage
from persisty.storage.storage_abc import T, StorageABC
from persisty.storage.storage_context_abc import StorageContextABC
from persisty.storage.storage_filter import StorageFilter
from persisty.storage.storage_meta import StorageMeta
from persisty.storage.wrappers.access_filtered_storage import with_access_filtered
from persisty.storage.wrappers.timestamped_storage import with_timestamps

PATTERN = re.compile('^[\\w_]+$')
GET_SEQUENCE_SQL = """'-- noinspection SqlResolve
SELECT COUNT(*) FROM sqlite_sequence WHERE name=?'"""


@dataclass(frozen=True)
class SqliteStorageContext(StorageContextABC):
    get_cursor: Callable[[], Any]
    storage: Dict[str, StorageABC] = field(default_factory=dict)
    access_control: AccessControlABC = AccessControl(
        is_meta_accessible=True,
        is_creatable=True,
        is_readable=True,
        is_updatable=False,
        is_destroyable=True,
        is_searchable=True
    )

    def register_storage(self, storage: StorageABC):
        self.storage[storage.item_type.__name__] = storage

    def get_storage(self, name: str) -> Optional[StorageABC]:
        storage = self.storage.get(name)
        if storage:
            return storage
        meta = self.read(name)
        if not meta:
            return None
        storage = table_storage(self.get_cursor, storage_meta=meta)
        storage = with_access_filtered(storage)
        storage = with_timestamps(storage)
        self.storage[name] = storage
        return storage

    def create(self, item: StorageMeta) -> str:
        if not self.access_control.is_creatable or not PATTERN.match(item.name):
            raise PersistyError(f'not_possible:{self.meta.name}:create')
        existing = self.read(item.name)
        if existing:
            raise PersistyError(f'existing_value:{item}')
        with self.get_cursor() as cursor:
            item_type = item.to_dataclass()
            sql_table = sql_table_from_type(item_type)
            create_sql = sql_table.create_table_sql()
            cursor.execute(create_sql)
        return item.name

    def read(self, key: str) -> Optional[StorageMeta]:
        if not PATTERN.match(key):
            return None
        storage = self.storage.get(key)
        if storage:
            return storage.meta
        with self.get_cursor() as cursor:
            table_info = cursor.execute(f'PRAGMA table_info({key})').fetchall()
            if not table_info:
                return None
            sql_cols = [SqlCol(name=row[1], sql_type=row[2], not_null=row[3]) for row in table_info]
            key_cols = [row[1] for row in table_info if row[5]]
            if len(key_cols) == 1:
                try:
                    is_auto_increment = bool(cursor.execute(GET_SEQUENCE_SQL, (key,)).fetchall())
                except OperationalError:
                    is_auto_increment = False
                key_generation = AttrMode.EXCLUDED if is_auto_increment else AttrMode.REQUIRED
                key_config = AttrKeyConfig(attr=key_cols[0], key_generation=key_generation)
            else:
                key_config = MultiKeyConfig(key_cols)
            attrs = tuple(c.to_attr() for c in sql_cols)
            meta = StorageMeta(name=key, attrs=attrs, key_config=key_config)
            return meta

    def update(self, item: StorageMeta) -> StorageMeta:
        # This is not supported due to the complexity of figuring out how to mutate the existing.
        raise PersistyError(f'not_possible:{self.meta.name}:update')

    def destroy(self, key: str) -> bool:
        if not PATTERN.match(key):
            return False
        try:
            with self.get_cursor() as cursor:
                cursor.execute('DROP TABLE ' + key)
                self.storage.pop(key, None)
                return True
        except (Exception, ValueError):
            return False

    def search(self, storage_filter: Optional[StorageFilter[StorageMeta]] = None) -> Iterator[StorageMeta]:
        with self.get_cursor() as cursor:
            storage_filter = self._create_storage_filter(storage_filter)
            for item in self._create_searcher().search(cursor, storage_filter):
                meta = self.read(item.name)
                yield meta

    def count(self, item_filter: Optional[ItemFilterABC[StorageMeta]] = None) -> int:
        with self.get_cursor() as cursor:
            item_filter = self._create_item_filter(item_filter)
            yield from self._create_searcher().count(cursor, item_filter)

    def paged_search(self, storage_filter: Optional[StorageFilter[StorageMeta]] = None, page_key: Optional[str] = None,
                     limit: int = 20) -> Page[T]:
        with self.get_cursor() as cursor:
            storage_filter = self._create_storage_filter(storage_filter)
            page = self._create_searcher().paged_search(cursor, storage_filter)
            items = [self.read(i.name) for i in page.items]
            return Page(items, page.next_page_key)

    @staticmethod
    def _create_searcher():
        cols = (SqlCol('type', True, 'VARCHAR(255)'), SqlCol('name', True, 'VARCHAR(255)'))
        sql_table = SqlTable(name='sqlite_master', cols=cols, key_col_name='name')
        searcher = table_searcher(sql_table, _SqliteMaster)
        return searcher

    def _create_storage_filter(self, storage_filter: Optional[StorageFilter[StorageMeta]]):
        if storage_filter:
            return StorageFilter(self._create_item_filter())
        else:
            return StorageFilter(self._create_item_filter(storage_filter.item_filter), storage_filter.item_comparator)

    @staticmethod
    def _create_item_filter(item_filter: Optional[ItemFilterABC] = None):
        type_filter = AttrFilter('type', AttrFilterOp.eq, 'table')
        if item_filter:
            return AndFilter((type_filter, item_filter))
        else:
            return type_filter


@dataclass
class _SqliteMaster:
    type: str
    name: str
