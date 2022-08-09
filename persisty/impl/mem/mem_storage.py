from typing import Optional, Dict, Iterator

from dataclasses import dataclass, field

from marshy.types import ExternalItemType

from persisty.access_control.constants import ALL_ACCESS
from persisty.errors import PersistyError
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.field.field import load_field_values
from persisty.storage.schema_validating_storage import SchemaValidatingStorage
from persisty.storage.secured_storage import SecuredStorage
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta

from persisty.util.undefined import UNDEFINED


@dataclass
class MemStorage(StorageABC):
    """
    In memory storage, used mostly for mocking and testing
    """

    storage_meta: StorageMeta = field()
    items: Dict[str, ExternalItemType] = field(default_factory=dict)

    def get_storage_meta(self) -> StorageMeta:
        return self.storage_meta

    def create(self, item: ExternalItemType) -> ExternalItemType:
        dumped = self._dump(item)
        key = self.storage_meta.key_config.to_key_str(item)
        if key is None:
            raise PersistyError(f"missing_key:{item}")
        if key in self.items:
            raise PersistyError(f"existing_value:{item}")
        self.items[key] = dumped
        return self._load(item)

    def read(self, key: str) -> Optional[ExternalItemType]:
        item = self.items.get(key)
        if item:
            item = self._load(item)
        return item

    def _update(
        self,
        key: str,
        item: ExternalItemType,
        updates: ExternalItemType,
        search_filter: SearchFilterABC = INCLUDE_ALL,
    ) -> Optional[ExternalItemType]:
        search_filter.validate_for_fields(self.storage_meta.fields)
        dumped = self._dump(updates, True)
        item.update(dumped)
        self.items[key] = item
        item = self._load(item)
        return item

    def _delete(self, key: str, item: ExternalItemType) -> bool:
        if key not in self.items:
            return False
        result = self.items.pop(key, UNDEFINED)
        return result is not UNDEFINED

    def search_all(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
    ) -> Iterator[ExternalItemType]:
        search_filter.validate_for_fields(self.storage_meta.fields)
        if search_order:
            search_order.validate_for_fields(self.storage_meta.fields)
        items = list(self.items.values())  # Copy to list prevents iterator bugs
        if search_filter is not INCLUDE_ALL:
            items = (
                item
                for item in items
                if search_filter.match(item, self.storage_meta.fields)
            )
        if search_order and search_order.orders:
            items = search_order.sort(items)
        items = (self._load(item) for item in items)
        return items

    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        search_filter.validate_for_fields(self.storage_meta.fields)
        if search_filter is INCLUDE_ALL:
            return len(self.items)
        count = sum(1 for _ in self.search_all(search_filter))
        return count

    def _load(self, item: ExternalItemType) -> ExternalItemType:
        return load_field_values(self.storage_meta.fields, item)

    def _dump(
        self, item: ExternalItemType, is_update: bool = False
    ) -> ExternalItemType:
        result = {}
        for field_ in self.storage_meta.fields:
            value = item.get(field_.name, UNDEFINED)
            if field_.write_transform:
                value = field_.write_transform.transform(value, is_update)
                item[field_.name] = value
            if value is not UNDEFINED:
                result[field_.name] = value
        return result


def mem_storage(
    storage_meta: StorageMeta, storage: Optional[Dict[str, ExternalItemType]] = None
):
    """Wraps a mem storage instance to provide additional checking"""
    if storage is None:
        storage = {}
    storage = MemStorage(storage_meta, storage)
    storage = SchemaValidatingStorage(storage)
    return storage
