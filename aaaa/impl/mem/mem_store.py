from typing import Optional, Dict, Iterator

from dataclasses import dataclass, field

from aaaa.errors import PersistyError
from aaaa.search_filter.include_all import INCLUDE_ALL
from aaaa.search_filter.search_filter_abc import SearchFilterABC
from aaaa.search_order.search_order import SearchOrder
from aaaa.store.store_abc import StoreABC, T
from aaaa.store_meta import StoreMeta

from aaaa.util.undefined import UNDEFINED


@dataclass
class MemStore(StoreABC[T]):
    """
    In memory store, used mostly for mocking, testing and caches. Relies on wrappers to provide access control,
    triggers and schema validation.
    """
    meta: StoreMeta = field()
    items: Dict[str, T] = field(default_factory=dict)

    def get_meta(self) -> StoreMeta:
        return self.meta

    def create(self, item: T) -> T:
        meta = self.meta
        key = self.meta.key_config.to_key_str(item)
        if key is None:
            raise PersistyError(f"missing_key:{item}")
        if key in self.items:
            raise PersistyError(f"existing_value:{item}")
        kwargs = {}
        for attr in meta.attrs:
            if attr.creatable:
                value = getattr(item, attr.name, UNDEFINED)
                create_transform = attr.create_transform
                if create_transform:
                    value = create_transform.transform(value)
                if value is not UNDEFINED:
                    kwargs[attr.name] = value
        stored_item = meta.get_stored_dataclass()(**kwargs)
        self.items[key] = stored_item
        return self._load(stored_item)

    def read(self, key: str) -> Optional[T]:
        item = self.items.get(key)
        if item:
            item = self._load(item)
        return item

    def _update(
        self,
        key: str,
        item: T,
        updates: T,
    ) -> Optional[T]:
        stored_item = self.items.get(key)
        if stored_item:
            for attr in self.meta.attrs:
                if attr.creatable:
                    value = getattr(item, attr.name, UNDEFINED)
                    update_transform = attr.update_transform
                    if update_transform:
                        value = update_transform.transform(value)
                    if value is not UNDEFINED:
                        setattr(stored_item, attr.name, value)
            return self._load(stored_item)

    def _delete(self, key: str, item: T) -> bool:
        if key not in self.items:
            return False
        result = self.items.pop(key, UNDEFINED)
        return result is not UNDEFINED

    def search_all(
        self,
        search_filter: SearchFilterABC[T] = INCLUDE_ALL,
        search_order: Optional[SearchOrder[T]] = None,
    ) -> Iterator[T]:
        search_filter = search_filter.lock_attrs(self.meta.attrs)
        if search_order:
            search_order.validate_for_attrs(self.meta.attrs)
        items = list(self.items.values())  # Copy to list prevents iterator bugs
        if search_filter is not INCLUDE_ALL:
            items = (
                item
                for item in items
                if search_filter.match(item, self.meta.attrs)
            )
        if search_order and search_order.orders:
            items = search_order.sort(items)
        items = (self._load(item) for item in items)
        return items

    def count(self, search_filter: SearchFilterABC[T] = INCLUDE_ALL) -> int:
        search_filter = search_filter.lock_attrs(self.meta.attrs)
        if search_filter is INCLUDE_ALL:
            return len(self.items)
        count = sum(1 for _ in self.search_all(search_filter))
        return count

    def _load(self, item: T) -> T:
        kwargs = {}
        for attr in self.meta.attrs:
            if attr.readable:
                kwargs[attr.name] = getattr(item, attr.name)
        result = self.meta.get_read_dataclass()(**kwargs)
        return result
