from dataclasses import dataclass
from typing import Tuple, Optional, Iterator

from marshy.types import ExternalItemType

from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta
from persisty.util.encrypt_at_rest import encrypt, decrypt


@dataclass(frozen=True)
class CompositeStorage(StorageABC):
    storage: Tuple[StorageABC, ...]
    storage_meta: StorageMeta

    def __post_init__(self):
        if not self.storage_meta:
            object.__setattr__(self, "storage_meta", self.storage[0].storage_meta)

    def create(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        key = self.get_storage_meta().key_config.get_key(item)
        if key:
            name, key = decrypt(key)
            storage = next(s for s in self.storage if s.get_storage_meta().name == name)
            storage.get_storage_meta().key_config.set_key(key, item)
            item = storage.create(item)
            self._set_key_after_read(item, self.storage_meta)
            return item
        for storage in self.storage:
            if storage.get_storage_meta().access_control.is_creatable(item):
                item = storage.create(item)
                self._set_key_after_read(item, self.storage_meta)
                return item

    def read(self, key: str) -> Optional[ExternalItemType]:
        name, key = decrypt(key)
        storage = next(s for s in self.storage if s.get_storage_meta().name == name)
        item = storage.read(key)
        self._set_key_after_read(item, storage.storage_meta)
        return item

    def update(
        self, updates: ExternalItemType, search_filter: SearchFilterABC = INCLUDE_ALL
    ) -> Optional[ExternalItemType]:
        key = self.get_storage_meta().key_config.get_key(updates)
        name, key = decrypt(key)
        storage = next(s for s in self.storage if s.get_storage_meta().name == name)
        storage.get_storage_meta().key_config.set_key(key, updates)
        item = storage.update(updates, search_filter)
        self._set_key_after_read(item, storage.storage_meta)
        return item

    def delete(self, key: str) -> bool:
        name, key = decrypt(key)
        storage = next(s for s in self.storage if s.get_storage_meta().name == name)
        return storage.delete(key)

    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        count = sum(storage.count(search_filter) for storage in self.storage)
        return count

    def search_all(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
    ) -> Iterator[ExternalItemType]:
        if not search_order:
            for storage in self.storage:
                yield from storage.search_all(search_filter)
            return
        iterators = [
            SubIterator(
                s.storage_meta, s.search_all(search_filter, search_order), search_order
            )
            for s in self.storage
        ]
        while next((i for i in iterators if i.next_item), None):
            iterators.sort()
            item = iterators[0].next_item
            item = self._set_key_after_read(item, iterators[0].storage_meta)
            yield item
            iterators[0].next()

    def _set_key_after_read(
        self, item: ExternalItemType, storage_meta: StorageMeta
    ) -> ExternalItemType:
        key = storage_meta.key_config.get_key(item)
        key = encrypt([storage_meta.name, key])
        self.get_storage_meta().key_config.set_key(key, item)
        return item


@dataclass
class SubIterator:
    storage_meta: StorageMeta
    iterator: Iterator[ExternalItemType]
    search_order: SearchOrder
    next_item: Optional[ExternalItemType] = None

    def __post_init__(self):
        if self.next_item is None:
            self.next_item = next(self.iterator, None)

    def __lt__(self, other):
        return self.search_order.lt(self.next_item, other.next_item)

    def __eq__(self, other):
        return self.search_order.eq(self.next_item, other.next_item)

    def next(self):
        self.next_item = next(self.iterator, None)
