from datetime import datetime
import itertools
from time import time

from dataclasses import dataclass, field
from typing import Optional, Iterator, Dict

from marshy import ExternalType, get_default_context
from marshy.marshaller.marshaller_abc import MarshallerABC

from persisty.cache_control.ttl_cache_control import TTLCacheControl
from persisty.cache_header import CacheHeader
from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.page import Page
from persisty.storage.storage_abc import StorageABC, T
from persisty.storage.storage_filter import StorageFilter
from persisty.storage.storage_meta import StorageMeta
from persisty.storage.wrappers.wrapper_storage_abc import WrapperStorageABC


@dataclass(frozen=True)
class TTLEntry:
    value: ExternalType
    expire_at: int


@dataclass(frozen=True)
class TTLCacheStorage(WrapperStorageABC[T]):
    """
    A storage with a local in memory cache, allowing for things to be a little bit stale in the name of performance.
    Intended for cases where processing reads the same items repeatedly from the local storage, allowing for simpler
    but performant business logic as the client code does not have to save references to the items.
    """
    wrapped_storage: StorageABC[T]
    ttl: int = 30
    batch_size: int = 100
    item_marshaller: MarshallerABC[T] = None
    _item_cache: Dict[str, TTLEntry] = field(default_factory=dict)

    def __post_init__(self):
        if self.item_marshaller is None:
            object.__setattr__(self, 'item_marshaller', get_default_context().get_marshaller(self.storage.item_type))

    def clear(self):
        """ Clear the cache """
        self._item_cache.clear()

    @property
    def storage(self) -> StorageABC[T]:
        return self.wrapped_storage

    @property
    def meta(self) -> StorageMeta:
        meta = self.wrapped_storage.meta
        return StorageMeta(
            name=meta.name,
            attrs=meta.attrs,
            key_config=meta.key_config,
            access_control=meta.access_control,
            cache_control=TTLCacheControl(self.ttl, meta.cache_control)
        )

    def _storage_item_in_cache(self, key: str, item: T):
        self._item_cache[key] = TTLEntry(self.item_marshaller.dump(item), int(time()) + self.ttl)

    def _load_item_from_cache(self, key: str):
        entry = self._item_cache.get(key)
        if entry and entry.expire_at > int(time()):
            return self.item_marshaller.load(entry.value)

    def get_cache_header(self, item: T) -> CacheHeader:
        cache_header = self.meta.cache_control.get_cache_header(item)
        expire_at = datetime.fromtimestamp(int(time()) + self.ttl)
        return CacheHeader(cache_header.etag, cache_header.updated_at, expire_at)

    def create(self, item: T) -> str:
        key = self.storage.create(item)
        self._storage_item_in_cache(key, item)
        return key

    def read(self, key: str) -> Optional[T]:
        item = self._load_item_from_cache(key)
        if item is None:
            item = self.storage.read(key)
            if item:
                self._storage_item_in_cache(key, item)
        return item

    def read_all(self, keys: Iterator[str], error_on_missing: bool = True) -> Iterator[T]:
        keys = iter(keys)
        while True:
            now = int(time())
            batch_keys = list(itertools.islice(keys, self.batch_size))
            if not batch_keys:
                return
            keys_not_in_cache = [k for k in batch_keys
                                 if k not in self._item_cache or self._item_cache[k].expire_at <= now]
            if keys_not_in_cache:
                for item in self.storage.read_all(keys_not_in_cache, error_on_missing):
                    if item is not None:
                        self._storage_item_in_cache(self.meta.key_config.get_key(item), item)
            for key in batch_keys:
                cache_entry = self._item_cache.get(key)
                item = None if cache_entry is None else self.item_marshaller.load(cache_entry.value)
                yield item

    def update(self, item: T) -> T:
        item = self.storage.update(item)
        key = self.meta.key_config.get_key(item)
        self._storage_item_in_cache(key, item)
        return item

    def destroy(self, key: str) -> bool:
        destroyed = self.storage.destroy(key)
        if destroyed and key in self._item_cache:
            del self._item_cache[key]
        return destroyed

    def search(self, storage_filter: Optional[StorageFilter[T]] = None) -> Iterator[T]:
        for item in self.storage.search(storage_filter):
            self._storage_item_in_cache(self.meta.key_config.get_key(item), item)
            yield item

    def paged_search(self,
                     storage_filter: Optional[StorageFilter[T]] = None,
                     page_key: str = None,
                     limit: int = 20
                     ) -> Page[T]:
        return self.storage.paged_search(storage_filter, page_key, limit)

    def edit_all(self, edits: Iterator[Edit[T]]):
        edits = self._edit_iterator(edits)
        return self.storage.edit_all(edits)

    def _edit_iterator(self, edits: Iterator[Edit[T]]):
        for edit in edits:
            if edit.edit_type == EditType.UPDATE:
                key = self.meta.key_config.get_key(edit.item)
                if key in self._item_cache:
                    del self._item_cache[key]
            elif edit.edit_type == EditType.DESTROY:
                if edit.key in self._item_cache:
                    del self._item_cache[edit.key]
            yield edit
