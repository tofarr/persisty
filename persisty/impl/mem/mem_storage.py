from typing import Optional, Dict, Iterator

from dataclasses import dataclass, field

from marshy.types import ExternalItemType

from persisty.errors import PersistyError
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order import SearchOrder
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta

from persisty.util.undefined import UNDEFINED


@dataclass
class MemStorage(StorageABC):
    """
    In memory storage, used mostly for mocking and testing
    """
    storage_meta: StorageMeta
    storage: Dict[str, ExternalItemType] = field(default_factory=dict)

    def create(self, item: ExternalItemType) -> ExternalItemType:
        dumped = self._dump(item)
        key = self.storage_meta.key_config.get_key(item)
        if key is None:
            raise PersistyError(f'missing_key:{item}')
        if key in self.storage:
            raise PersistyError(f'existing_value:{item}')
        self.storage[key] = dumped
        return self._load(item)

    def read(self, key: str) -> Optional[ExternalItemType]:
        item = self.storage.get(key)
        if item:
            item = self._load(item)
        return item

    def update(self,
               item: ExternalItemType,
               search_filter: SearchFilterABC = INCLUDE_ALL
               ) -> Optional[ExternalItemType]:
        key = self.storage_meta.key_config.get_key(item)
        if key is None:
            raise PersistyError(f'missing_key:{item}')
        stored = self.storage.get(key)
        if not stored or not search_filter.match(stored, self.storage_meta.fields):
            return None
        dumped = self._dump(item, True)
        stored.update(dumped)
        item = self._load(stored)
        return item

    def delete(self, key: str) -> bool:
        if key not in self.storage:
            return False
        result = self.storage.pop(key, UNDEFINED)
        return result is not UNDEFINED

    def search_all(self,
                   search_filter: SearchFilterABC = INCLUDE_ALL,
                   search_order: Optional[SearchOrder] = None
                   ) -> Iterator[ExternalItemType]:
        search_filter.validate_for_fields(self.storage_meta.fields)
        search_order.validate_for_fields(self.storage_meta.fields)
        items = iter(self.storage.values())
        if search_filter is not INCLUDE_ALL:
            items = (item for item in items if search_filter.match(item, self.storage_meta.fields))
        if search_order:
            items = search_order.sort(items)
        items = (self._load(item) for item in items)
        return items

    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        search_filter.validate_for_fields(self.storage_meta.fields)
        if search_filter is None:
            return len(self.storage)
        count = sum(1 for _ in self.search_all(search_filter))
        return count

    def _load(self, item: ExternalItemType) -> ExternalItemType:
        result = {}
        for field_ in self.storage_meta.fields:
            if not field_.is_readable:
                continue
            result[field_.name] = item.get(field_.name, UNDEFINED)
        return result

    def _dump(self, item: ExternalItemType, is_update: bool = False) -> ExternalItemType:
        result = {}
        for field_ in self.storage_meta.fields:
            if is_update and not field_.is_updatable:
                continue
            if not is_update and not field_.is_creatable:
                continue
            value = item.get(field_.name, UNDEFINED)
            if field_.write_transform:
                value = field_.write_transform.transform(value, is_update)
                item[field_.name] = value
            if value is not UNDEFINED:
                result[field_.name] = value
        return result
