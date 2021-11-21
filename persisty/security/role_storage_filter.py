from dataclasses import dataclass
from typing import Generic, Optional

from persisty.access_control.access_control import ALL_ACCESS
from persisty.access_control.access_control_abc import AccessControlABC
from persisty.item_filter.item_filter_abc import ItemFilterABC, T
from persisty.security.role_check import RoleCheck
from persisty.storage.storage_abc import StorageABC
from persisty.storage.wrappers.access_filtered_storage import AccessFilteredStorage
from persisty.storage.wrappers.filtered_storage import FilteredStorage


@dataclass(frozen=True)
class RoleStorageFilter(Generic[T]):
    role_check: RoleCheck
    access_control: AccessControlABC = ALL_ACCESS
    item_filter: Optional[ItemFilterABC[T]] = None

    def filter_storage(self, storage: StorageABC[T]):
        if self.access_control != ALL_ACCESS:
            storage = AccessFilteredStorage(storage, self.access_control)
        if self.item_filter is not None:
            storage = FilteredStorage(storage, self.item_filter)
        return storage
