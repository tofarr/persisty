from dataclasses import dataclass
from typing import Tuple, Optional, Iterator

from marshy.types import ExternalItemType

from persisty.errors import PersistyError
from persisty.search_filter.and_filter import And
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.not_filter import Not
from persisty.search_filter.or_filter import Or
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.field.field import Field
from persisty.field.field_filter import FieldFilter
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta


@dataclass(frozen=True)
class CompositeStorage(StorageABC):
    storage: Tuple[StorageABC, ...]
    storage_meta: StorageMeta = None
    key_delimiter: str = "/"

    def __post_init__(self):
        if not self.storage_meta:
            object.__setattr__(self, "storage_meta", self.storage[0].get_storage_meta())

    def get_storage_meta(self) -> StorageMeta:
        return self.storage_meta

    def split_key(self, key: str) -> Tuple[str, str]:
        storage_name, sub_key = key.split(self.key_delimiter, 2)
        return storage_name, sub_key

    def merge_key(self, storage_name: str, sub_key: str) -> str:
        key = storage_name + self.key_delimiter + sub_key
        return key

    def storage_for_key(self, key: str) -> Tuple[StorageABC, str]:
        storage_name, sub_key = self.split_key(key)
        storage = next(
            (s for s in self.storage if s.get_storage_meta().name == storage_name), None
        )
        if not storage:
            raise PersistyError(f"no_such_storage:{storage_name}")
        return storage, sub_key

    def apply_key_to_item(self, storage: StorageABC, item: ExternalItemType):
        sub_key = storage.get_storage_meta().key_config.to_key_str(item)
        key = self.merge_key(storage.get_storage_meta().name, sub_key)
        self.get_storage_meta().key_config.from_key_str(key, item)
        return item

    def search_filter_for_storage(
        self, search_filter: SearchFilterABC, sub_storage: StorageABC
    ) -> SearchFilterABC:
        if search_filter in (INCLUDE_ALL, EXCLUDE_ALL):
            return search_filter
        elif isinstance(search_filter, And) or isinstance(search_filter, Or):
            return search_filter.__class__(
                search_filters=tuple(
                    self.search_filter_for_storage(f, sub_storage)
                    for f in search_filter.search_filters
                )
            )
        elif isinstance(search_filter, Not):
            return Not(
                self.search_filter_for_storage(search_filter.search_filter, sub_storage)
            )
        elif isinstance(search_filter, FieldFilter):
            if not sub_storage.get_storage_meta().key_config.is_required_field(
                search_filter.name
            ):
                return search_filter
        return TransformedFilter(self, sub_storage, search_filter)

    def create(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        key = self.get_storage_meta().key_config.to_key_str(item)
        if key:
            storage, sub_key = self.storage_for_key(key)
            storage.get_storage_meta().key_config.from_key_str(sub_key, item)
            item = storage.create(item)
            self.apply_key_to_item(storage, item)
            return item
        for storage in self.storage:
            if storage.get_storage_meta().access_control.is_creatable(item):
                item = storage.create(item)
                self.apply_key_to_item(storage, item)
                return item

    def read(self, key: str) -> Optional[ExternalItemType]:
        storage, sub_key = self.storage_for_key(key)
        item = storage.read(sub_key)
        if item:
            self.apply_key_to_item(storage, item)
        return item

    def update(
        self, updates: ExternalItemType, search_filter: SearchFilterABC = INCLUDE_ALL
    ) -> Optional[ExternalItemType]:
        key = self.get_storage_meta().key_config.to_key_str(updates)
        storage, sub_key = self.storage_for_key(key)
        updates = storage.get_storage_meta().key_config.from_key_str(
            sub_key, {**updates}
        )
        item = storage.update(updates, search_filter)
        if item:
            self.apply_key_to_item(storage, item)
        return item

    def delete(self, key: str) -> bool:
        storage, sub_key = self.storage_for_key(key)
        return storage.delete(sub_key)

    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        count = 0
        for sub_storage in self.storage:
            storage_search_filter = self.search_filter_for_storage(
                search_filter, sub_storage
            )
            count += sub_storage.count(storage_search_filter)
        return count

    def search_all(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
    ) -> Iterator[ExternalItemType]:
        if not search_order:
            for sub_storage in self.storage:
                storage_search_filter = self.search_filter_for_storage(
                    search_filter, sub_storage
                )
                items = (
                    self.apply_key_to_item(sub_storage, item)
                    for item in sub_storage.search_all(storage_search_filter)
                )
                yield from items
            return
        iterators = [
            SubIterator(
                self,
                s,
                s.search_all(
                    self.search_filter_for_storage(search_filter, s), search_order
                ),
                search_order,
            )
            for s in self.storage
        ]
        while True:
            iterators.sort()
            if not next((i for i in iterators if i.next_item), None):
                return
            item = iterators[0].next_item
            yield item
            iterators[0].next()


@dataclass
class SubIterator:
    composite_storage: CompositeStorage
    sub_storage: StorageABC
    iterator: Iterator[ExternalItemType]
    search_order: SearchOrder
    next_item: Optional[ExternalItemType] = None

    def __post_init__(self):
        self.next()

    def __lt__(self, other):
        if not self.next_item:
            return not bool(other.next_item)
        elif not other.next_item:
            return True
        return self.search_order.lt(self.next_item, other.next_item)

    def next(self):
        next_item = next(self.iterator, None)
        if next_item:
            self.composite_storage.apply_key_to_item(self.sub_storage, next_item)
        self.next_item = next_item


@dataclass(frozen=True)
class TransformedFilter(SearchFilterABC):
    composite_storage: CompositeStorage
    sub_storage: StorageABC
    search_filter: SearchFilterABC

    def validate_for_fields(self, fields: Tuple[Field, ...]) -> bool:
        return self.search_filter.validate_for_fields(fields)

    def match(self, item: ExternalItemType, fields: Tuple[Field, ...]) -> bool:
        item = {**item}
        self.composite_storage.apply_key_to_item(self.sub_storage, item)
        matched = self.search_filter.match(item, fields)
        return matched
