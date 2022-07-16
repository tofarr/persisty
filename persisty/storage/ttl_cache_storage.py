from copy import deepcopy
from time import time
from typing import Optional, Dict, List

from dataclasses import dataclass, field

from marshy.types import ExternalType, ExternalItemType

from persisty.storage.batch_edit import BatchEditABC, Delete, Update
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.result_set import ResultSet
from persisty.storage.search_filter.include_all import INCLUDE_ALL
from persisty.storage.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.search_order import SearchOrder, NO_ORDER
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta
from persisty.util import secure_hash


@dataclass(frozen=True)
class TTLEntry:
    value: ExternalType
    expire_at: int


@dataclass(frozen=True)
class TTLCacheStorage(StorageABC):
    storage: StorageABC
    cache: Dict[str, TTLEntry] = field(default_factory=dict)
    cached_result_sets: Dict[str, TTLEntry] = field(default_factory=dict)
    ttl: int = 30

    def clear_cache(self):
        self.cache.clear()
        self.cached_result_sets.clear()

    @property
    def storage_meta(self) -> StorageMeta:
        return self.storage.storage_meta

    def store_item_in_cache(self, key: str, item: ExternalItemType, expire_at: Optional[int] = None):
        if expire_at is None:
            expire_at = int(time()) + self.ttl
        self.cache[key] = TTLEntry(deepcopy(item), expire_at)

    def load_item_from_cache(self, key: str, now: Optional[int] = None):
        if now is None:
            now = int(time())
        entry = self.cache.get(key)
        if entry and entry.expire_at > now:
            return deepcopy(entry.value)

    def create(self, item: ExternalItemType) -> ExternalItemType:
        item = self.storage.create(item)
        key = self.storage_meta.key_config.get_key(item)
        self.store_item_in_cache(key, item)
        return item

    def read(self, key: str) -> Optional[ExternalItemType]:
        item = self.load_item_from_cache(key)
        if item is None:
            item = self.storage.read(key)
            if item:
                self.store_item_in_cache(key, item)
        return item

    def read_batch(self, keys: List[str]) -> List[Optional[ExternalItemType]]:
        now = int(time())
        items_by_key = {key: self.load_item_from_cache(key, now) for key in keys}
        keys_to_load = [key for key, item in items_by_key.values() if item is None]
        if keys_to_load:
            items = await self.storage.read_batch(keys_to_load)
            key_config = self.storage_meta.key_config
            for item in items:
                if item:
                    key = key_config.get_key(item)
                    self.store_item_in_cache(key, item, now)
                    items_by_key[key] = item
        items = [items_by_key.get(key) for key in keys]
        return items

    def update(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        item = self.storage.update(item)
        key = self.storage_meta.key_config.get_key(item)
        self.store_item_in_cache(key, item)
        return item

    def delete(self, key: str) -> bool:
        destroyed = self.storage.delete(key)
        if key in self.cache:
            del self.cache[key]
        return destroyed

    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        return self.storage.count(search_filter)

    def search(self,
               search_filter: SearchFilterABC = INCLUDE_ALL,
               search_order: SearchOrder = NO_ORDER,
               page_key: Optional[str] = None,
               limit: Optional[int] = None
               ) -> ResultSet:
        result_set_key = [deepcopy(i) for i in (search_filter, search_order, page_key, limit)]
        result_set_key = secure_hash(result_set_key)
        now = int(time())
        entry = self.cached_result_sets.get(result_set_key)
        if entry and entry.expire_at > now:
            results = [self.load_item_from_cache(key, now) for key in entry.value['keys']]
            return ResultSet(results, entry.value['next_page_key'])
        result_set = self.storage.search(search_filter, search_order, page_key, limit)
        key_config = self.storage_meta.key_config
        expire_at = int(time()) + self.ttl
        keys = []
        for item in result_set.results:
            key = key_config.get_key(item)
            self.store_item_in_cache(key, item, expire_at)
            keys.append(key)
        entry = dict(keys=keys, next_page_key=result_set.next_page_key)
        self.cached_result_sets[result_set_key] = TTLEntry(entry, expire_at)
        return result_set

    async def edit_batch(self, edits: List[BatchEditABC]) -> List[BatchEditResult]:
        results = await self.storage.edit_batch(edits)
        for result in results:
            if not result.success:
                continue
            edit = result.edit
            if isinstance(edit, Update):
                key = self.storage_meta.key_config.get_key(edit.updates)
                self.cache.pop(key, None)
            elif isinstance(edit, Delete):
                self.cache.pop(edit.key, None)
        return results
