from dataclasses import dataclass
from typing import Optional, Tuple

from marshy.types import ExternalItemType

from persisty.access_control.access_control import NO_ACCESS
from persisty.errors import PersistyError
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta
from persisty.storage.wrapper_storage_abc import WrapperStorageABC


@dataclass(frozen=True)
class SecuredStorage(WrapperStorageABC):
    storage: StorageABC
    storage_meta: StorageMeta = None

    def __post_init__(self):
        if not self.storage_meta:
            object.__setattr__(self, 'storage_meta', self.storage.storage_meta)

    def filter_create(self, item: ExternalItemType) -> ExternalItemType:
        if not self.storage_meta.access_control.is_creatable(item):
            raise PersistyError('create_forbidden')
        return item

    def filter_update(self, old_item: ExternalItemType, updates: ExternalItemType) -> ExternalItemType:
        if self.storage_meta.access_control.is_updatable(old_item, updates):
            return updates

    def filter_read(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        if self.storage_meta.access_control.is_readable(item):
            return item

    def allow_delete(self, item: ExternalItemType) -> bool:
        return self.storage_meta.access_control.is_deletable(item)

    def filter_search_filter(self, search_filter: SearchFilterABC) -> Tuple[SearchFilterABC, bool]:
        access_control = self.storage_meta.access_control
        if not access_control.is_searchable():
            return NO_ACCESS, True
        return access_control.transform_search_filter(search_filter)
