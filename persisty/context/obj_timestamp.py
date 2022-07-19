from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, List, Iterator

from marshy.marshaller_context import MarshallerContext
from marshy.types import ExternalItemType

from persisty.context.obj_storage_meta import META_KEY_CONFIG
from persisty.obj_storage.obj_storage import ObjStorage
from persisty.obj_storage.obj_storage_abc import ObjStorageABC
from persisty.search_filter.search_filter_factory_abc import SearchFilterFactoryABC
from persisty.search_order.search_order_factory_abc import SearchOrderFactoryABC

from persisty.search_filter import INCLUDE_ALL, SearchFilterABC
from persisty.storage.batch_edit import BatchEditABC
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.storage_abc import StorageABC
from persisty.storage.wrapper_storage_abc import WrapperStorageABC
from persisty.obj_storage.stored import stored


@stored
class StorageTimestamp:
    name: str
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    items_updated_at: Optional[datetime]


@dataclass
class TimestampSearchFilter(SearchFilterFactoryABC):
    pass


class TimestampSearchField(Enum):
    NAME = 'name'
    CREATED_AT = 'created_at'
    UPDATED_AT = 'updated_at'
    ITEMS_UPDATED_AT = 'items_updated_at'


@dataclass
class TimestampSearchOrder(SearchOrderFactoryABC):
    field: TimestampSearchField = None
    desc: bool = False


@dataclass
class TimestampCreateInput:
    name: str


@dataclass
class TimestampUpdateInput:
    name: str
    content_updated_at: str


TimestampStorageABC = ObjStorageABC[StorageTimestamp, TimestampSearchFilter, TimestampSearchOrder, TimestampCreateInput,
                                    TimestampUpdateInput]


def timestamp_storage(storage: StorageABC, marshaller_context: MarshallerContext) -> TimestampStorageABC:
    return ObjStorage(
        storage=storage,
        item_marshaller=marshaller_context.get_marshaller(StorageTimestamp),
        key_config=META_KEY_CONFIG,
        search_filter_factory_type=TimestampSearchFilter,
        search_order_factory_type=TimestampSearchOrder,
        create_input_marshaller=marshaller_context.get_marshaller(TimestampCreateInput),
        update_input_marshaller=marshaller_context.get_marshaller(TimestampUpdateInput)
    )


@dataclass(frozen=True)
class TimestampUpdateStorage(WrapperStorageABC):
    storage: StorageABC
    timestamp_storage: StorageABC

    def create(self, item: ExternalItemType) -> ExternalItemType:
        item = self.get_storage().create(item)
        self.update_timestamp()
        return item

    def update(self,
               updates: ExternalItemType,
               search_filter: SearchFilterABC = INCLUDE_ALL
               ) -> Optional[ExternalItemType]:
        item = self.get_storage().update(updates)
        if item:
            self.update_timestamp()
        return item

    def delete(self, key: str) -> bool:
        result = self.get_storage().delete(key)
        if result:
            self.update_timestamp()
        return result

    async def edit_batch(self, edits: List[BatchEditABC]) -> List[BatchEditResult]:
        result = await self.get_storage().edit_batch(edits)
        if next((True for r in result if r.success), False):
            self.update_timestamp()
        return result

    def edit_all(self, edits: Iterator[BatchEditABC]) -> Iterator[BatchEditResult]:
        update_timestamp = False
        for result in self.get_storage().edit_all(edits):
            yield result
            update_timestamp |= result.success
        if update_timestamp:
            self.update_timestamp()

    def update_timestamp(self):
        self.timestamp_storage.update(dict(
            name=self.get_storage_meta().name,
            content_updated_at=datetime.now().isoformat()
        ))
