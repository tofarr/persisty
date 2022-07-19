from typing import Optional, List, Any

from dataclasses import dataclass, field
from marshy.types import ExternalItemType
from sqlalchemy.orm import Session

from persisty.impl.sqlalchemy.engine import get_engine
from persisty.storage.batch_edit import BatchEditABC
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta


zzz zzzz zzzz

from sqlalchemy import MetaData, Table, Column, Integer, ForeignKey, String, and_, select, func

metadata_obj = MetaData()

address_table = Table(
    "address",
    metadata_obj,
    Column('id', Integer, primary_key=True),
    Column('user_id', ForeignKey('user_account.id'), nullable=False),
    Column('email_address', String, nullable=False)
)

address_table.select().where(address_table.columns.get('user_id') == 10)


@dataclass(frozen=True)
class SqlAlchemyTableStorage(StorageABC):
    """
    This class uses sql alchemy at a lower level than the standard orm usage
    """
    storage_meta: StorageMeta
    # SqlAlchemy engine class
    table: Table
    engine: Any = field(default_factory=get_engine)

    def create(self, item: ExternalItemType) -> ExternalItemType:
        self.table.select
        pass

    def read(self, key: str) -> Optional[ExternalItemType]:
        stmt = self.table.select(whereclause=self._key_where_clause(key))
        with Session(get_engine()) as session:
            row = session.execute(stmt).first()
            item = {c.id: str(v) for c, v in zip(stmt.columns, row)}
            return item

    def update(self, updates: ExternalItemType) -> Optional[ExternalItemType]:
        pass

    def delete(self, key: str) -> bool:
        stmt = self.table.delete(whereclause=self._key_where_clause(key))
        with Session(get_engine()) as session:
            row = session.execute(stmt)
            return bool(row[0])

    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        stmt = select([func.count()]).select_from(self.table)

    async def read_batch(self, keys: List[str]) -> List[Optional[ExternalItemType]]:
        pass

    def search(self,
               search_filter: SearchFilterABC = INCLUDE_ALL,
               search_order: Optional[SearchOrder] = None,
               page_key: Optional[str] = None,
               limit: Optional[int] = None
               ) -> ResultSet[ExternalItemType]:
        pass

    def edit_batch(self, edits: List[BatchEditABC]) -> List[BatchEditResult]:
        pass

    def _key_where_clause(self, key: str):
        key_dict = {}
        self.get_storage_meta().key_config.set_key(key, key_dict)
        conditions = []
        for k, v in key_dict.items():
            conditions.append(self.table.columns.get(k) == v)
        return and_(conditions)
