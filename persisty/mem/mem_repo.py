import itertools
from dataclasses import dataclass, field
from typing import Optional, Iterator, Type, Dict

from marshy.default_context import new_default_context
from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.marshaller_context import MarshallerContext
from marshy.types import ExternalItemType

from persisty.capabilities import Capabilities, ALL_CAPABILITIES
from persisty.mem.mem_search_filter import MemSearchFilter
from persisty.page import Page
from persisty.errors import PersistyError
from persisty.repo_abc import RepoABC, F, T


@dataclass(frozen=True)
class MemRepo(RepoABC[T, F]):
    """ In memory repo. Useful for caching and mocking """
    marshaller: MarshallerABC[T]
    mem_search_filter: MemSearchFilter
    key_attr: str = 'id'
    store: Dict[str, ExternalItemType] = field(default_factory=dict)

    def get_item_type(self) -> Type[T]:
        return self.marshaller.marshalled_type

    def get_capabilities(self) -> Capabilities:
        return ALL_CAPABILITIES

    def get_key(self, item: T) -> str:
        key = str(getattr(item, self.key_attr))
        return key

    def create(self, item: T) -> str:
        key = self.get_key(item)
        dumped = self.marshaller.dump(item)
        if key in self.store:
            raise PersistyError(f'existing_value:{item}')
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

    def search(self, search_filter: Optional[F] = None) -> Iterator[T]:
        filtered_results = self.mem_search_filter.filter_results(search_filter, iter(self.store.values()))
        items = (self.marshaller.load(item) for item in filtered_results)
        return items

    def count(self, search_filter: Optional[F] = None) -> int:
        items = self.search(search_filter)
        count = sum(1 for _ in items)
        return count

    def paginated_search(self, search_filter: Optional[F] = None, page_key: str = None, limit: int = 20) -> Page[T]:
        items = self.search(search_filter)
        if page_key is not None:
            while True:
                item = next(items)
                if self.get_key(item) == page_key:
                    break
        page_items = list(itertools.islice(items, limit))
        next_page_key = self.get_key(page_items[-1]) if len(page_items) == limit else None
        return Page(page_items, next_page_key)


def mem_repo(item_type: Type[T],
             filter_type: Type[F],
             marshaller_context: Optional[MarshallerContext] = None
             ) -> MemRepo[T, F]:
    if marshaller_context is None:
        marshaller_context = new_default_context()
    marshaller = marshaller_context.get_marshaller(item_type)
    filter_marshaller = marshaller_context.get_marshaller(filter_type)
    mem_search_filter = MemSearchFilter(filter_marshaller)
    return MemRepo(marshaller.marshalled_type.__name__, marshaller, mem_search_filter)
