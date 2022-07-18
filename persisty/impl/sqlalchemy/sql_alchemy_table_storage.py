from typing import Optional, List, Any

from dataclasses import dataclass, field
from marshy.types import ExternalItemType

from persisty.impl.sqlalchemy.engine import get_engine
from persisty.storage.batch_edit import BatchEditABC
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.result_set import ResultSet
from persisty.storage.search_filter import SearchFilterABC, INCLUDE_ALL
from persisty.storage.search_order import SearchOrderABC
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta


zzz zzzz zzzz

from sqlalchemy import MetaData, Table, Column, Integer, ForeignKey, String
metadata_obj = MetaData()

address_table = Table(
    "address",
    metadata_obj,
    Column('id', Integer, primary_key=True),
    Column('user_id', ForeignKey('user_account.id'), nullable=False),
    Column('email_address', String, nullable=False)
)




@dataclass(frozen=True)
class SqlAlchemyTableStorage(StorageABC):
    storage_meta: StorageMeta
    # SqlAlchemy engine class
    table: Table
    engine: Any = field(default_factory=get_engine)

    def create(self, item: ExternalItemType) -> ExternalItemType:
        self.table.select
        pass

    def read(self, key: str) -> Optional[ExternalItemType]:
        pass

    def update(self, updates: ExternalItemType) -> Optional[ExternalItemType]:
        pass

    def delete(self, key: str) -> bool:
        pass

    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        pass

    async def read_batch(self, keys: List[str]) -> List[Optional[ExternalItemType]]:
        pass

    def search(self,
               search_filter: SearchFilterABC = INCLUDE_ALL,
               search_order: Optional[SearchOrderABC] = None,
               page_key: Optional[str] = None,
               limit: Optional[int] = None
               ) -> ResultSet[ExternalItemType]:
        pass

    def edit_batch(self, edits: List[BatchEditABC]) -> List[BatchEditResult]:
        pass