from dataclasses import dataclass
from typing import Optional, List

from persisty.batch_edit import BatchEditABC, Create, Update, Delete
from persisty.result_set import ResultSet
from persisty.search_filter.all_items import ALL_ITEMS
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order_abc import SearchOrderABC
from persisty.security.authorization import Authorization
from persisty.storage.storage_abc import StorageABC, T, F, C
from persisty.storage.storage_meta import StorageMeta


class SecurityError(Exception):
    pass


@dataclass(frozen=True)
class SecuredStorage(StorageABC[T, F, C]):
    storage: StorageABC[T, F, C]
    authorization: Authorization

    @property
    def storage_meta(self) -> StorageMeta:
        return self.storage.storage_meta

    def create(self, item: T) -> T:
        access_control = self.storage_meta.access_control
        if not access_control.is_creatable(self.authorization):
            raise SecurityError('create_forbidden')
        return self.storage.create(item)

    def _check_read(self):
        access_control = self.storage_meta.access_control
        if not access_control.is_readable(self.authorization):
            raise SecurityError('read_forbidden')

    def read(self, key: str) -> Optional[T]:
        self._check_read()
        return self.storage.read(key)

    async def read_batch(self, keys: List[str]) -> List[Optional[T]]:
        self._check_read()
        return await self.storage.read_batch(keys)

    def update(self, item: T) -> Optional[T]:
        access_control = self.storage_meta.access_control
        if not access_control.is_updatable(self.authorization):
            raise SecurityError('update_forbidden')
        return self.storage.update(item)

    def delete(self, key: str) -> bool:
        access_control = self.storage_meta.access_control
        if not access_control.is_deletable(self.authorization):
            raise SecurityError('delete_forbidden')
        return self.storage.delete(key)

    def search(self,
               search_filter: SearchFilterABC = ALL_ITEMS,
               search_order: Optional[SearchOrderABC] = None,
               page_key: Optional[str] = None,
               limit: Optional[int] = None
               ) -> ResultSet[T]:
        access_control = self.storage_meta.access_control
        if not access_control.is_searchable(self.authorization):
            raise SecurityError('search_forbidden')
        return self.storage.search(search_filter, search_order, page_key, limit)

    def count(self, search_filter: SearchFilterABC = ALL_ITEMS) -> int:
        access_control = self.storage_meta.access_control
        if not access_control.is_searchable(self.authorization):
            raise SecurityError('search_forbidden')
        return self.storage.count(search_filter)

    async def edit_batch(self, edits: List[BatchEditABC]):
        has_create = False
        has_update = False
        has_delete = False
        for edit in edits:
            has_create |= isinstance(edit, Create)
            has_update |= isinstance(edit, Update)
            has_delete |= isinstance(edit, Delete)
        access_control = self.storage_meta.access_control
        if has_create and not access_control.is_creatable(self.authorization):
            raise SecurityError('create_forbidden')
        if has_update and not access_control.is_updatable(self.authorization):
            raise SecurityError('update_forbidden')
        if has_delete and not access_control.is_deletable(self.authorization):
            raise SecurityError('delete_forbidden')
        return self.storage.edit_batch(edits)
