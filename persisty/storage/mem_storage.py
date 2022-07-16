from copy import deepcopy
from typing import Optional, Dict, Iterator

from dataclasses import dataclass, field

from marshy.types import ExternalItemType

from persisty.errors import PersistyError
from persisty.storage.search_filter.include_all import INCLUDE_ALL
from persisty.storage.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.search_order import SearchOrder, NO_ORDER
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta

from persisty.util.undefined import UNDEFINED


@dataclass(frozen=True)
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
        return item

    def read(self, key: str) -> Optional[ExternalItemType]:
        item = self.storage.get(key)
        if item:
            item = deepcopy(item)
        return item

    def update(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        key = self.storage_meta.key_config.get_key(item)
        if key is None:
            raise PersistyError(f'missing_key:{item}')
        stored = self.storage.get(key)
        if not stored:
            return None
        dumped = self._dump(item, True)
        stored.update(dumped)
        item = deepcopy(stored)
        return item

    def delete(self, key: str) -> bool:
        if key not in self.storage:
            return False
        result = self.storage.pop(key, UNDEFINED)
        return result is not UNDEFINED

    def search_all(self,
                   search_filter: SearchFilterABC = INCLUDE_ALL,
                   search_order: SearchOrder = NO_ORDER
                   ) -> Iterator[ExternalItemType]:
        items = iter(self.storage.values())
        if search_filter is not INCLUDE_ALL:
            items = (item for item in items if search_filter.match(item, self.storage_meta.fields))
        if search_order:
            items = sorted(items, key=search_order.key)
        items = (deepcopy(item) for item in items)
        return items

    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        if search_filter is None:
            return len(self.storage)
        count = sum(1 for _ in self.search_all(search_filter))
        return count

    def _dump(self, item: ExternalItemType, is_update: bool = False) -> ExternalItemType:
        result = {}
        for field_ in self.storage_meta.fields:
            value = item.get(field_.name, UNDEFINED)
            if field_.write_transform:
                value = field_.write_transform.transform(value, is_update)
                item[field_.name] = value
            if value is not UNDEFINED:
                result[field_.name] = value
        return result
