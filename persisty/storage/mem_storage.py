from itertools import islice
from typing import Optional, Dict, Iterator

from dataclasses import dataclass, field
from marshy.marshaller_context import MarshallerContext
from marshy.types import ExternalItemType

from persisty.errors import PersistyError
from persisty.search_filter.all_items import ALL_ITEMS
from persisty.storage.result_set import ResultSet
from persisty.storage.storage_abc import StorageABC, T, F, S
from persisty.storage.storage_meta import StorageMeta

from marshy import get_default_context, dump

from persisty.util.undefined import UNDEFINED


@dataclass(frozen=True)
class MemStorage(StorageABC[T, F, S]):
    """
    In memory storage, used mostly for mocking and testing
    """
    storage_meta: StorageMeta
    storage: Dict[str, ExternalItemType] = field(default_factory=dict)
    marshy_context: MarshallerContext = get_default_context()

    def create(self, item: T) -> T:
        key_config = self.storage_meta.key_config
        dumped = self._dump(item)
        key = key_config.get_key(item)
        if key is None:
            raise PersistyError(f'missing_key:{item}')
        if key in self.storage:
            raise PersistyError(f'existing_value:{item}')
        self.storage[key] = dumped
        return item

    def read(self, key: str) -> Optional[T]:
        item = self.storage.get(key)
        if item:
            item = self.marshy_context.load(self.storage_meta.item_type, key)
        return item

    def update(self, item: T) -> Optional[T]:
        key_config = self.storage_meta.key_config
        dumped = self._dump(item, True)
        key = key_config.get_key(item)
        if key is None:
            raise PersistyError(f'missing_key:{item}')
        if key not in self.storage:
            return None
        self.storage[key] = dumped
        return item

    def delete(self, key: str) -> bool:
        if key not in self.storage:
            return False
        result = self.storage.pop(key, UNDEFINED)
        return result is not UNDEFINED

    def search(self, search_filter: Optional[F] = None, search_order: Optional[S] = None,
               page_key: Optional[str] = None, limit: Optional[int] = None) -> ResultSet[T]:
        assert(limit <= self.storage_meta.batch_size)
        items = self.search_all(search_filter, search_order)

        if page_key:
            while True:
                next_result = next(items, None)
                if next_result is None:
                    return ResultSet([])
                key = self.storage_meta.key_config.get_key(next_result)
                if key == page_key:
                    break

        items = list(islice(items, limit))

        page_key = None
        if len(items) == limit:
            page_key = self.storage_meta.key_config.get_key(items[-1])

        return ResultSet(items, page_key)

    def search_all(self,
                   search_filter: Optional[F] = None,
                   search_order: Optional[S] = None
                   ) -> Iterator[T]:
        items = (self.marshy_context.load(self.storage_meta.item_type, item) for item in self.storage.values())
        items = self.filter_items(items, search_filter)
        if search_order:
            items = sorted(items, key=search_order.key_for_fields(self.storage_meta.fields))
        return items

    def count(self, search_filter: Optional[F] = None) -> int:
        if search_filter is None:
            return len(self.storage)
        items = (self.marshy_context.load(self.storage_meta.item_type, item) for item in self.storage.values())
        items = self.filter_items(items, search_filter)
        count = sum(1 for _ in items)
        return count

    def _dump(self, item: T, is_update: bool = False) -> ExternalItemType:
        result = {}
        for field_ in self.storage_meta.fields:
            value = field_.__get__(item, item.__class__)
            if field_.generator:
                value = field_.generator.generate_value(value, is_update)
                field_.__set__(item, value)
            if value is not UNDEFINED:
                dumped = dump(value, field_.type)
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
