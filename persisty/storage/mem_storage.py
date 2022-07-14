from typing import Optional, Dict

from dataclasses import dataclass, field
from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.types import ExternalItemType

from persisty.result_set import ResultSet

from persisty.errors import PersistyError
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order_abc import SearchOrderABC
from persisty.storage.storage_abc import StorageABC, T, F, C
from persisty.storage.storage_meta import StorageMeta


@dataclass(frozen=True)
class MemStorage(StorageABC[T, F, C]):
    storage_meta: StorageMeta
    marshaller: MarshallerABC[T]
    storage: Dict[str, ExternalItemType] = field(default_factory=dict)

    def create(self, item: T) -> T:
        key_config = self.storage_meta.key_config
        key = key_config.get_key(item)
        if key is None:
            key = key_config.generate_key()
            key_config.set_key(item, key)
        if key in self.storage:
            raise PersistyError(f'existing_value:{item}')
        dumped = self.marshaller.dump(item)
        self.storage[key] = dumped
        return item THIS DOESNT DO ANY ATTRIBUTE UPDATES THE WAY SQL WOULD - STOPPED HERE

    def read(self, key: str) -> Optional[T]:
        pass

    def update(self, item: T) -> Optional[T]:
        pass

    def delete(self, key: str) -> bool:
        pass

    def search(self, search_filter: SearchFilterABC = ALL_ITEMS, search_order: Optional[SearchOrderABC] = None,
               page_key: Optional[str] = None, limit: Optional[int] = None) -> ResultSet[T]:
        pass

    def count(self, search_filter: SearchFilterABC = ALL_ITEMS) -> int:
        pass