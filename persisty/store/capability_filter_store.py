from dataclasses import dataclass
from typing import Optional, Iterator, Any

from persisty.capabilities import Capabilities, ALL_CAPABILITIES
from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.errors import PersistyError
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.page import Page
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC, T
from persisty.store_schemas import StoreSchemas


@dataclass(frozen=True)
class CapabilityFilterStore(WrapperStoreABC[T]):
    wrapped_store: StoreABC[T]
    capabilities: Capabilities = ALL_CAPABILITIES

    def __post_init__(self):
        object.__setattr__(self, 'capabilities', self.capabilities & self.store.capabilities)

    @property
    def store(self):
        return self.wrapped_store

    @property
    def schemas(self) -> StoreSchemas[T]:
        schemas = self.store.schemas
        capabilities = self.capabilities
        return StoreSchemas(
            create=schemas.create if capabilities.create else None,
            update=schemas.update if capabilities.update else None,
            read=schemas.read if capabilities.read or capabilities.search else None
        )

    def create(self, item: T) -> str:
        if not self.capabilities.create:
            raise PersistyError(f'not_possible:{self.name}:create')
        return self.store.create(item)

    def read(self, key: str) -> Optional[T]:
        if not self.capabilities.read:
            raise PersistyError(f'not_possible:{self.name}:read')
        return self.store.read(key)

    def read_all(self, keys: Iterator[str], error_on_missing: bool = True) -> Iterator[T]:
        if not self.capabilities.read:
            raise PersistyError(f'not_possible:{self.name}:read')
        return self.store.read_all(keys, error_on_missing)

    def update(self, item: T) -> T:
        if not self.capabilities.update:
            raise PersistyError(f'not_possible:{self.name}:update')
        return self.store.update(item)

    def destroy(self, key: str) -> bool:
        if not self.capabilities.destroy:
            raise PersistyError(f'not_possible:{self.name}:destroy')
        return self.store.destroy(key)

    def search(self, search_filter: Any = None) -> Iterator[T]:
        if not self.capabilities.search:
            raise PersistyError(f'not_possible:{self.name}:read')
        return self.store.search(search_filter)

    def count(self, item_filter: Optional[ItemFilterABC[T]] = None) -> int:
        if not self.capabilities.search:
            raise PersistyError(f'not_possible:{self.name}:read')
        return self.store.count(item_filter)

    def paged_search(self, search_filter: Any = None, page_key: str = None, limit: int = 20) -> Page[T]:
        if not self.capabilities.search:
            raise PersistyError(f'not_possible:{self.name}:read')
        return self.store.paged_search(search_filter, page_key, limit)

    def edit_all(self, edits: Iterator[Edit[T]]):
        edits = self._filter_edits(edits)
        return self.store.edit_all(edits)

    def _filter_edits(self, edits: Iterator[Edit[T]]) -> Iterator[Edit[T]]:
        for edit in edits:
            if edit.edit_type == EditType.CREATE:
                if not self.capabilities.create:
                    raise PersistyError(f'not_possible:{self.name}:create')
            elif edit.edit_type == EditType.UPDATE:
                if not self.capabilities.update:
                    raise PersistyError(f'not_possible:{self.name}:update')
            elif edit.edit_type == EditType.DESTROY:
                if not self.capabilities.destroy:
                    raise PersistyError(f'not_possible:{self.name}:destroy')
            yield edit
