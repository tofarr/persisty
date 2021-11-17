import itertools
from dataclasses import fields, dataclass, field
from typing import Optional, Iterator, Type, Dict

from marshy import get_default_context
from marshy.marshaller_context import MarshallerContext

from persisty.access_control.access_control import AccessControl
from persisty.attr.attr import attr_from_field
from persisty.errors import PersistyError
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.key_config.key_generation import KeyGeneration
from persisty.page import Page
from persisty.storage.in_mem.in_mem_storage import InMemStorage
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_filter import StorageFilter
from persisty.storage.storage_meta import StorageMeta

KEY_CONFIG = AttrKeyConfig(KeyGeneration.SPECIFIED, 'name')


@dataclass
class InMemMetaStorage(StorageABC[StorageMeta]):
    marshaller_context: MarshallerContext = field(default_factory=get_default_context)
    stores: Dict[str, InMemStorage] = field(default_factory=dict)

    @property
    def item_type(self) -> Type[StorageMeta]:
        return StorageMeta

    @property
    def meta(self) -> StorageMeta:
        return StorageMeta(
            name=StorageMeta.__name__,
            attrs=tuple(attr_from_field(f) for f in fields(StorageMeta)),
            key_config=KEY_CONFIG,
            access_control=AccessControl(is_creatable=True, is_readable=False, is_destroyable=False,
                                         is_searchable=False)
        )

    def create(self, item: StorageMeta) -> str:
        if item.name in self.stores:
            raise PersistyError(f'existing_value:{item}')
        item_type = item.to_dataclass()
        marshaller = self.marshaller_context.get_marshaller(item_type)
        self.stores[item.name] = InMemStorage(item, marshaller)

    def read(self, key: str) -> Optional[StorageMeta]:
        storage = self.stores.get(key)
        return storage.meta if storage else None

    def update(self, item: StorageMeta) -> StorageMeta:
        raise PersistyError('unsupported')

    def destroy(self, key: str) -> bool:
        if key in self.stores:
            del self.stores[key]
            return True
        return False

    def search(self, storage_filter: Optional[StorageFilter[StorageMeta]] = None) -> Iterator[StorageMeta]:
        items = (m.storage_meta for m in self.stores.values())
        if storage_filter:
            items = storage_filter.filter_items(items)
        return items

    def count(self, item_filter: Optional[ItemFilterABC[StorageMeta]] = None) -> int:
        items = self.search(StorageFilter(item_filter))
        count = sum(1 for _ in items)
        return count

    def paged_search(self, storage_filter: Optional[StorageFilter[StorageMeta]] = None, page_key: Optional[str] = None,
                     limit: int = 20) -> Page[StorageMeta]:
        items = self.search(storage_filter)
        page_items = list(itertools.islice(items, limit))
        next_page_key = KEY_CONFIG.get_key(page_items[-1]) if len(page_items) == limit else None
        return Page(page_items, next_page_key)
