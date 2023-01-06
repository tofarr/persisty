from dataclasses import dataclass
from typing import Optional, Tuple

from marshy.types import ExternalItemType
from servey.security.authorization import Authorization

from persisty.access_control.access_control import AccessControl
from persisty.errors import PersistyError
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.filtered_storage_abc import FilteredStorageABC
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta


@dataclass(frozen=True)
class SecuredStorage(FilteredStorageABC):
    storage: StorageABC
    access_control: AccessControl

    def get_storage(self) -> StorageABC:
        return self.storage

    def get_storage_meta(self) -> StorageMeta:
        return self.storage.get_storage_meta()

    def filter_create(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        if not self.access_control.is_creatable(item):
            raise PersistyError("create_forbidden")
        return item

    def filter_update(
        self, old_item: ExternalItemType, updates: ExternalItemType
    ) -> ExternalItemType:
        if self.access_control.is_updatable(old_item, updates):
            return updates

    def filter_read(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        if self.access_control.is_readable(item):
            return item

    def allow_delete(self, item: ExternalItemType) -> bool:
        return self.access_control.is_deletable(item)

    def filter_search_filter(
        self, search_filter: SearchFilterABC
    ) -> Tuple[SearchFilterABC, bool]:
        if not self.access_control.is_searchable():
            return EXCLUDE_ALL, True
        return self.access_control.transform_search_filter(search_filter)


def secured_storage(storage: StorageABC, authorization: Authorization) -> StorageABC:
    access_controls = (
        f.create_access_control(authorization)
        for f in storage.get_storage_meta().access_control_factories
    )
    access_controls = (a for a in access_controls if a)
    access_control = next(access_controls)
    return SecuredStorage(storage, access_control)
