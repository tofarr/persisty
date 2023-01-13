from copy import deepcopy
from time import time
from typing import Optional, Dict, List, Generic

from dataclasses import dataclass, attr

from marshy import dump, get_default_context
from marshy.marshaller.marshaller_abc import MarshallerABC

from persisty.batch_edit import BatchEdit
from persisty.batch_edit_result import BatchEditResult
from persisty.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.store.store_abc import StoreABC, T
from persisty.store_meta import StoreMeta
from persisty.util import secure_hash


@dataclass(frozen=True)
class TtlEntry(Generic[T]):
    value: T
    expire_at: int


@dataclass(frozen=True)
class TtlCacheStore(StoreABC[T]):
    store: StoreABC[T]
    cache: Dict[str, TtlEntry] = attr(default_factory=dict)
    cached_result_sets: Dict[str, TtlEntry] = attr(default_factory=dict)
    ttl: int = 30
    marshaller: MarshallerABC[T] = None

    def __post_init__(self):
        if not self.marshaller:
            object.__setattr__(
                self,
                "marshaller",
                get_default_context().get_marshaller(
                    self.get_meta().get_read_dataclass()
                ),
            )

    def clear_cache(self):
        self.cache.clear()
        self.cached_result_sets.clear()

    def get_meta(self) -> StoreMeta:
        return self.store.get_meta()

    def store_item_in_cache(self, key: str, item: T, expire_at: Optional[int] = None):
        if expire_at is None:
            expire_at = int(time()) + self.ttl
        self.cache[key] = TtlEntry(deepcopy(item), expire_at)

    def load_item_from_cache(self, key: str, now: Optional[int] = None):
        if now is None:
            now = int(time())
        entry = self.cache.get(key)
        if entry and entry.expire_at > now:
            return deepcopy(entry.value)

    def create(self, item: T) -> Optional[T]:
        item = self.store.create(item)
        key = self.get_meta().key_config.to_key_str(item)
        self.store_item_in_cache(key, item)
        return item

    def read(self, key: str) -> Optional[T]:
        item = self.load_item_from_cache(key)
        if item is None:
            item = self.store.read(key)
            if item:
                self.store_item_in_cache(key, item)
        return item

    def read_batch(self, keys: List[str]) -> List[Optional[T]]:
        now = int(time())
        items_by_key = {key: self.load_item_from_cache(key, now) for key in keys}
        keys_to_load = [key for key, item in items_by_key.items() if item is None]
        if keys_to_load:
            items = self.store.read_batch(keys_to_load)
            key_config = self.get_meta().key_config
            for item in items:
                if item:
                    key = key_config.to_key_str(item)
                    self.store_item_in_cache(key, item, now)
                    items_by_key[key] = item
        items = [items_by_key.get(key) for key in keys]
        return items

    def _update(
        self,
        key: str,
        item: T,
        updates: T,
    ) -> Optional[T]:
        item = self.store._update(key, item, updates)
        if item:
            self.store_item_in_cache(key, item)
        return item

    def _delete(self, key: str, item: T) -> bool:
        destroyed = self.store._delete(key, item)
        if key in self.cache:
            del self.cache[key]
        return destroyed

    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        return self.store.count(search_filter)

    def search(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[T]:
        result_set_key = [
            dump(search_filter, SearchFilterABC),
            dump(search_order, Optional[SearchOrder]),
            page_key,
            limit,
        ]
        result_set_key = secure_hash(result_set_key)
        now = int(time())
        entry = self.cached_result_sets.get(result_set_key)
        if entry and entry.expire_at > now:
            results = [
                self.load_item_from_cache(key, now) for key in entry.value["keys"]
            ]
            return ResultSet(results, entry.value["next_page_key"])
        result_set = self.store.search(search_filter, search_order, page_key, limit)
        key_config = self.get_meta().key_config
        expire_at = int(time()) + self.ttl
        keys = []
        for item in result_set.results:
            key = key_config.to_key_str(item)
            self.store_item_in_cache(key, item, expire_at)
            keys.append(key)
        entry = dict(keys=keys, next_page_key=result_set.next_page_key)
        self.cached_result_sets[result_set_key] = TtlEntry(entry, expire_at)
        return result_set

    def edit_batch(self, edits: List[BatchEdit]) -> List[BatchEditResult]:
        results = self.store.edit_batch(edits)
        for result in results:
            if not result.success:
                continue
            edit = result.edit
            if edit.update_item:
                key = self.get_meta().key_config.to_key_str(edit.update_item)
                self.cache.pop(key, None)
            elif edit.delete_key:
                self.cache.pop(edit.delete_key, None)
        return results
