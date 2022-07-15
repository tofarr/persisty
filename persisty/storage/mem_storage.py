from typing import Optional, Dict, Iterator

from dataclasses import dataclass, field

from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.types import ExternalItemType

from persisty.errors import PersistyError
from persisty.search_filter.all_items import ALL_ITEMS
from persisty.storage.storage_abc import StorageABC, T, F, S
from persisty.storage.storage_meta import StorageMeta

from persisty.util.undefined import UNDEFINED


@dataclass(frozen=True)
class MemStorage(StorageABC[T, F, S]):
    """
    In memory storage, used mostly for mocking and testing
    """
    storage_meta: StorageMeta
    marshaller: MarshallerABC[T]
    storage: Dict[str, ExternalItemType] = field(default_factory=dict)

    def create(self, item: T) -> T:
        dumped = self._dump(item)
        key = self.storage_meta.key_config.get_key(item)
        if key is None:
            raise PersistyError(f'missing_key:{item}')
        if key in self.storage:
            raise PersistyError(f'existing_value:{item}')
        self.storage[key] = dumped
        return item

    def read(self, key: str) -> Optional[T]:
        item = self.storage.get(key)
        if item:
            item = self.marshaller.load(item)
        return item

    def update(self, item: T) -> Optional[T]:
        key = self.storage_meta.key_config.get_key(item)
        if key is None:
            raise PersistyError(f'missing_key:{item}')
        stored = self.storage.get(key)
        if not stored:
            return None
        dumped = self._dump(item, True)
        stored.update(dumped)
        item = self.marshaller.load(stored)
        return item

    def delete(self, key: str) -> bool:
        if key not in self.storage:
            return False
        result = self.storage.pop(key, UNDEFINED)
        return result is not UNDEFINED

    def search_all(self, search_filter: Optional[F] = None, search_order: Optional[S] = None) -> Iterator[T]:
        items = (self.marshaller.load(item) for item in self.storage.values())
        items = self.filter_items(items, search_filter)
        if search_order:
            items = sorted(items, key=search_order.key_for_fields(self.storage_meta.fields))
        return items

    def count(self, search_filter: Optional[F] = None) -> int:
        if search_filter is None:
            return len(self.storage)
        count = sum(1 for _ in self.search_all(search_filter))
        return count

    def _dump(self, item: T, is_update: bool = False) -> ExternalItemType:
        result = {}
        for field_ in self.storage_meta.fields:
            value = field_.__get__(item, item.__class__)
            if field_.generator:
                value = field_.generator.generate_value(value, is_update)
                field_.__set__(item, value)
            if value is not UNDEFINED:
                dumped = self.marshaller.dump(value)
                result[field_.name] = dumped
        return result

    def filter_items(self, items: Iterator[T], search_filter: Optional[F]) -> Iterator[T]:
        if search_filter is not None:
            if hasattr(search_filter, 'create_item_filter'):
                item_filter = search_filter.create_item_filter(self.storage_meta.fields, self.storage_meta.item_type)
                if item_filter is not ALL_ITEMS:
                    items = (item for item in items if item_filter.match(item))
            else:
                items = (item for item in items if search_filter.match(self.storage_meta.fields, item))
        return items
