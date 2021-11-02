import itertools
from dataclasses import dataclass, field
from typing import Optional, Iterator, Type, Dict
from uuid import uuid4

from marshy.default_context import new_default_context
from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.marshaller_context import MarshallerContext
from marshy.types import ExternalItemType

from persisty.capabilities import Capabilities, ALL_CAPABILITIES
from persisty.search_filter import SearchFilter
from persisty.page import Page
from persisty.errors import PersistyError
from persisty.store.store_abc import StoreABC, T
from persisty.store_schemas import StoreSchemas, schemas_for_type, NO_SCHEMAS


@dataclass(frozen=True)
class InMemStore(StoreABC[T]):
    """ In memory store. Useful for caching and mocking """
    marshaller: MarshallerABC[T]
    key_attr: str = 'id'
    store: Dict[str, ExternalItemType] = field(default_factory=dict)
    name: str = None

    def __post_init__(self):
        if self.name is None:
            object.__setattr__(self, 'name', self.marshaller.marshalled_type.__name__)

    @property
    def item_type(self) -> Type[T]:
        return self.marshaller.marshalled_type

    @property
    def capabilities(self) -> Capabilities:
        return ALL_CAPABILITIES

    @property
    def schemas(self) -> StoreSchemas[T]:
        return NO_SCHEMAS

    def get_key(self, item: T) -> str:
        key = getattr(item, self.key_attr)
        if key is not None:
            key = str(key)
        return key

    def create(self, item: T) -> str:
        key = self.get_key(item)
        if key is None:
            key = str(uuid4())
            setattr(item, self.key_attr, key)
        if key in self.store:
            raise PersistyError(f'existing_value:{item}')
        dumped = self.marshaller.dump(item)
        self.store[key] = dumped
        return key

    def read(self, key: str) -> Optional[T]:
        item = self.store.get(key)
        if item is None:
            return None
        loaded = self.marshaller.load(item)
        return loaded

    def update(self, item: T) -> T:
        key = self.get_key(item)
        if key not in self.store:
            raise PersistyError(f'missing_value:{item}')
        dumped = self.marshaller.dump(item)
        self.store[key] = dumped
        return item

    def destroy(self, key: str) -> bool:
        if key not in self.store:
            return False
        del self.store[key]
        return True

    def search(self, search_filter: Optional[SearchFilter] = None) -> Iterator[T]:
        items = [self.marshaller.load(item) for item in self.store.values()]
        if search_filter:
            items = search_filter.filter_items(items)
        return iter(items)

    def count(self, search_filter: Optional[SearchFilter] = None) -> int:
        items = self.search(search_filter)
        count = sum(1 for _ in items)
        return count

    def paged_search(self,
                     search_filter: Optional[SearchFilter] = None,
                     page_key: Optional[str] = None,
                     limit: int = 20
                     ) -> Page[T]:
        items = self.search(search_filter)
        if page_key is not None:
            while True:
                item = next(items)
                if self.get_key(item) == page_key:
                    break
        page_items = list(itertools.islice(items, limit))
        next_page_key = self.get_key(page_items[-1]) if len(page_items) == limit else None
        return Page(page_items, next_page_key)


def in_mem_store(item_type: Type[T],
                 key_attr: Optional[str] = 'id',
                 marshaller_context: Optional[MarshallerContext] = None
                 ) -> InMemStore[T]:
    if marshaller_context is None:
        marshaller_context = new_default_context()
    marshaller = marshaller_context.get_marshaller(item_type)
    return InMemStore(marshaller, key_attr=key_attr, name=marshaller.marshalled_type.__name__)
