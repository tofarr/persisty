from dataclasses import dataclass
from typing import Optional, Iterator, Any

from persisty.access_control.access_control import AccessControl, ALL_ACCESS
from persisty.access_control.access_control_abc import AccessControlABC
from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.errors import PersistyError
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.page import Page
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta
from persisty.storage.wrappers.wrapper_storage_abc import WrapperStorageABC, T


@dataclass(frozen=True)
class AccessFilteredStorage(WrapperStorageABC[T]):
    wrapped_storage: StorageABC[T]
    access_control: AccessControlABC

    def __post_init__(self):
        wrapped_access_control = self.meta.access_control
        access_control = self.access_control
        if isinstance(wrapped_access_control, AccessControl) and isinstance(access_control, AccessControl):
            access_control = access_control & wrapped_access_control
            object.__setattr__(self, 'access_control', access_control)

    @property
    def storage(self):
        return self.wrapped_storage

    @property
    def meta(self) -> StorageMeta:
        meta = self.storage.meta
        return StorageMeta(
            name=meta.name,
            attrs=meta.attrs,
            key_config=meta.key_config,
            access_control=self.access_control,
            cache_control=meta.cache_control
        )

    def create(self, item: T) -> str:
        if not self.access_control.is_creatable:
            raise PersistyError(f'not_possible:{self.meta.name}:create')
        return self.storage.create(item)

    def read(self, key: str) -> Optional[T]:
        if not self.access_control.is_readable:
            raise PersistyError(f'not_possible:{self.meta.name}:read')
        return self.storage.read(key)

    def read_all(self, keys: Iterator[str], error_on_missing: bool = True) -> Iterator[T]:
        if not self.access_control.is_readable:
            raise PersistyError(f'not_possible:{self.meta.name}:read')
        return self.storage.read_all(keys, error_on_missing)

    def update(self, item: T) -> T:
        if not self.access_control.is_updatable:
            raise PersistyError(f'not_possible:{self.meta.name}:update')
        return self.storage.update(item)

    def destroy(self, key: str) -> bool:
        if not self.access_control.is_destroyable:
            raise PersistyError(f'not_possible:{self.meta.name}:destroy')
        return self.storage.destroy(key)

    def search(self, storage_filter: Any = None) -> Iterator[T]:
        if not self.access_control.is_searchable:
            raise PersistyError(f'not_possible:{self.meta.name}:read')
        return self.storage.search(storage_filter)

    def count(self, item_filter: Optional[ItemFilterABC[T]] = None) -> int:
        if not self.access_control.is_searchable:
            raise PersistyError(f'not_possible:{self.meta.name}:read')
        return self.storage.count(item_filter)

    def paged_search(self, storage_filter: Any = None, page_key: str = None, limit: int = 20) -> Page[T]:
        if not self.access_control.is_searchable:
            raise PersistyError(f'not_possible:{self.meta.name}:read')
        return self.storage.paged_search(storage_filter, page_key, limit)

    def edit_all(self, edits: Iterator[Edit[T]]):
        edits = self._filter_edits(edits)
        return self.storage.edit_all(edits)

    def _filter_edits(self, edits: Iterator[Edit[T]]) -> Iterator[Edit[T]]:
        for edit in edits:
            if edit.edit_type == EditType.CREATE:
                if not self.access_control.is_creatable:
                    raise PersistyError(f'not_possible:{self.meta.name}:create')
            elif edit.edit_type == EditType.UPDATE:
                if not self.access_control.is_updatable:
                    raise PersistyError(f'not_possible:{self.meta.name}:update')
            elif edit.edit_type == EditType.DESTROY:
                if not self.access_control.is_destroyable:
                    raise PersistyError(f'not_possible:{self.meta.name}:destroy')
            yield edit


def with_access_filtered(storage: StorageABC, access_control: Optional[AccessControlABC] = None):
    if access_control is None:
        access_control = storage.meta.access_control
    if access_control != ALL_ACCESS:
        storage = AccessFilteredStorage(storage, access_control)
    return storage
