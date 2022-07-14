from dataclasses import field, dataclass, MISSING
from typing import Optional, Iterator, Dict, Union, Type

from marshy import get_default_context
from marshy.marshaller_context import MarshallerContext

from persisty.access_control.access_control import AccessControl
from persisty.access_control.access_control_abc import AccessControlABC
from persisty.errors import PersistyError
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.page import Page
from persisty.storage.in_mem.in_mem_storage import InMemStorage, items_to_page
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_context_abc import StorageContextABC
from persisty.storage.storage_filter import StorageFilter
from persisty.storage.storage_meta import StorageMeta
from persisty.storage.wrappers.access_filtered_storage import with_access_filtered
from persisty.storage.wrappers.timestamped_storage import with_timestamps


@dataclass(frozen=True)
class InMemStorageContext(StorageContextABC):
    marshaller_context: MarshallerContext = field(default_factory=get_default_context)
    storage: Dict[str, StorageABC] = field(default_factory=dict)
    access_control: AccessControlABC = AccessControl(
        is_meta_accessible=True,
        is_creatable=True,
        is_readable=True,
        is_updatable=False,
        is_destroyable=True,
        is_searchable=True
    )

    def get_storage(self, key: Union[str, Type]) -> Optional[StorageABC]:
        if not isinstance(key, str):
            key = key.__name__
        storage = self.storage.get(key)
        return storage

    def register_storage(self, storage: StorageABC):
        self.storage[storage.item_type.__name__] = storage

    def create(self, item: StorageMeta) -> str:
        if not self.access_control.is_creatable:
            raise PersistyError(f'not_possible:{self.meta.name}:create')
        if item.name in self.storage:
            raise PersistyError(f'existing_value:{item}')
        item_type = item.to_dataclass()
        marshaller = self.marshaller_context.get_marshaller(item_type)
        storage = InMemStorage(item, marshaller)
        storage = with_access_filtered(storage)
        storage = with_timestamps(storage)
        return storage

    def read(self, key: str) -> Optional[StorageMeta]:
        if not self.access_control.is_readable:
            raise PersistyError(f'not_possible:{self.meta.name}:read')
        storage = self.storage.get(key)
        return storage.meta if storage else None

    def update(self, item: StorageMeta) -> StorageMeta:
        # This is not supported due to the complexity of figuring out how to mutate the existing.
        raise PersistyError(f'not_possible:{self.meta.name}:update')

    def destroy(self, key: str) -> bool:
        if not self.access_control.is_readable:
            raise PersistyError(f'not_possible:{self.meta.name}:destroy')
        result = self.storage.pop(key, MISSING)
        return result is not MISSING

    def search(self, storage_filter: Optional[StorageFilter[StorageMeta]] = None) -> Iterator[StorageMeta]:
        items = (s.meta for s in self.storage.values())
        if storage_filter:
            items = storage_filter.filter_items(items)
        return iter(items)

    def count(self, item_filter: Optional[ItemFilterABC[StorageMeta]] = None) -> int:
        if not item_filter:
            return len(self.storage)
        items = self.search(StorageFilter(item_filter))
        count = sum(1 for _ in items)
        return count

    def paged_search(self, storage_filter: Optional[StorageFilter[StorageMeta]] = None, page_key: Optional[str] = None,
                     limit: int = 20) -> Page[StorageMeta]:
        items = self.search(storage_filter)
        page = items_to_page(items, self.meta.key_config, page_key, limit)
        return page
