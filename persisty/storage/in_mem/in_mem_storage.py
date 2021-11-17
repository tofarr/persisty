import itertools
from dataclasses import dataclass, field
from typing import Optional, Iterator, Type, Dict
from uuid import uuid4

from marshy import get_default_context
from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.types import ExternalItemType

from persisty.errors import PersistyError
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.page import Page
from persisty.storage.storage_abc import StorageABC, T
from persisty.storage.storage_filter import StorageFilter
from persisty.storage.storage_meta import StorageMeta, storage_meta_from_dataclass


@dataclass(frozen=True)
class InMemStorage(StorageABC[T]):
    """ In memory storage. Useful for caching and mocking """
    storage_meta: StorageMeta
    marshaller: MarshallerABC[T]
    storage: Dict[str, ExternalItemType] = field(default_factory=dict)

    @property
    def meta(self) -> StorageMeta:
        return self.storage_meta

    @property
    def item_type(self) -> Type[T]:
        return self.marshaller.marshalled_type

    def create(self, item: T) -> str:
        key = self.meta.key_config.get_key(item)
        if key is None:
            key = str(uuid4())
            self.meta.key_config.set_key(item, key)
        if key in self.storage:
            raise PersistyError(f'existing_value:{item}')
        dumped = self.marshaller.dump(item)
        self.storage[key] = dumped
        return key

    def read(self, key: str) -> Optional[T]:
        item = self.storage.get(key)
        if item is None:
            return None
        loaded = self.marshaller.load(item)
        return loaded

    def update(self, item: T) -> T:
        key = self.meta.key_config.get_key(item)
        if key not in self.storage:
            raise PersistyError(f'missing_value:{item}')
        dumped = self.marshaller.dump(item)
        self.storage[key] = dumped
        return item

    def destroy(self, key: str) -> bool:
        if key not in self.storage:
            return False
        del self.storage[key]
        return True

    def search(self, storage_filter: Optional[StorageFilter] = None) -> Iterator[T]:
        items = [self.marshaller.load(item) for item in self.storage.values()]
        if storage_filter:
            items = storage_filter.filter_items(items)
        return iter(items)

    def count(self, item_filter: Optional[ItemFilterABC[T]] = None) -> int:
        items = self.search(StorageFilter(item_filter))
        count = sum(1 for _ in items)
        return count

    def paged_search(self,
                     storage_filter: Optional[StorageFilter] = None,
                     page_key: Optional[str] = None,
                     limit: int = 20
                     ) -> Page[T]:
        items = self.search(storage_filter)
        page = items_to_page(items, self.meta.key_config, page_key, limit)
        return page


def in_mem_storage(item_type: Type[T]) -> InMemStorage[T]:
    marshaller = get_default_context().get_marshaller(item_type)
    storage_meta = storage_meta_from_dataclass(item_type)
    return InMemStorage(storage_meta, marshaller)


def items_to_page(items: Iterator[T], key_config: KeyConfigABC, page_key: Optional[str] = None,
                  limit: int = 20 ) -> Page[T]:
    if page_key is not None:
        while True:
            item = next(items)
            if key_config.get_key(item) == page_key:
                break
    page_items = list(itertools.islice(items, limit))
    next_page_key = key_config.get_key(page_items[-1]) if len(page_items) == limit else None
    return Page(page_items, next_page_key)
