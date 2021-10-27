import itertools
from time import time

from dataclasses import dataclass, field
from typing import Optional, Iterator, Any, Dict

from marshy import ExternalType, get_default_context
from marshy.marshaller.marshaller_abc import MarshallerABC

from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.page import Page
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC, T


@dataclass(frozen=True)
class TTLEntry:
    value: ExternalType
    expire_at: int


@dataclass(frozen=True)
class TTLCacheStore(WrapperStoreABC[T]):
    """
    Allows things to be a little bit stale in the name of performance.
    """
    store: StoreABC[T]
    timeout: int = 30
    batch_size: int = 100
    item_marshaller: MarshallerABC[T] = None
    _item_cache: Dict[str, TTLEntry] = field(default_factory=dict)

    def __post_init__(self):
        if self.item_marshaller is None:
            object.__setattr__(self, 'item_marshaller', get_default_context().get_marshaller(self.store.item_type))

    def _store_item_in_cache(self, key: str, item: T):
        self._item_cache[key] = TTLEntry(self.item_marshaller.dump(item), int(time()) + self.timeout)

    def _load_item_from_cache(self, key: str):
        entry = self._item_cache.get(key)
        if entry and entry.expire_at > int(time()):
            return self.item_marshaller.load(entry.value)

    def create(self, item: T) -> str:
        key = self.store.create(item)
        self._store_item_in_cache(key, item)
        return key

    def read(self, key: str) -> Optional[T]:
        item = self._load_item_from_cache(key)
        if item is None:
            item = self.store.read(key)
            if item:
                self._store_item_in_cache(key, item)
        return item

    def read_all(self, keys: Iterator[str], error_on_missing: bool = True) -> Iterator[T]:
        while True:
            now = int(time())
            batch_keys = list(itertools.islice(keys, self.batch_size))
            keys_not_in_cache = [k for k in keys if k not in self._item_cache or self._item_cache[k].expire_at <= now]
            if keys_not_in_cache:
                for item in self.store.read_all(keys_not_in_cache, error_on_missing):
                    self._store_item_in_cache(self.get_key(item), item)
            for key in batch_keys:
                yield self.item_marshaller.load(self._item_cache[key].value)

    def update(self, item: T) -> T:
        item = self.store.update(item)
        key = self.get_key(item)
        self._store_item_in_cache(key, item)
        return item

    def destroy(self, key: str) -> bool:
        destroyed = self.store.destroy(key)
        if destroyed and key in self._item_cache:
            del self._item_cache[key]
        return destroyed

    def search(self, search_filter: Any = None) -> Iterator[T]:
        for item in self.store.search(search_filter):
            self._store_item_in_cache(self.get_key(item), item)
            yield item

    def paged_search(self, search_filter: Any = None, page_key: str = None, limit: int = 20) -> Page[T]:
        return self.store.paged_search(search_filter, page_key, limit)

    def edit_all(self, edits: Iterator[Edit[T]]):
        return self.store.edit_all(edits)

    def _edit_iterator(self, edits: Iterator[Edit[T]]):
        for edit in edits:
            if edit.edit_type == EditType.UPDATE:
                key = self.get_key(edit.edit_type)
                if key in self._item_cache:
                    del self._item_cache[key]
            elif edit.edit_type == EditType.DESTROY:
                if edit.key in self._item_cache:
                    del self._item_cache[edit.key]
            yield edit
