from dataclasses import dataclass
from typing import Generic, Optional

from old.persisty.capabilities import Capabilities, ALL_CAPABILITIES
from persisty.item_filter.item_filter_abc import ItemFilterABC, T
from old.persisty.storage.capability_filter_storage import CapabilityFilterStorage
from old.persisty.storage.storage_filter_storage import StorageFilterStorage
from persisty.security.role_check import RoleCheck
from old.persisty.storage.storage_abc import StorageABC


@dataclass(frozen=True)
class RoleStorageFilter(Generic[T]):
    role_check: RoleCheck
    capabilities: Capabilities = ALL_CAPABILITIES
    item_filter: Optional[ItemFilterABC[T]] = None

    def filter_storage(self, storage: StorageABC[T]):
        if self.capabilities != ALL_CAPABILITIES:
            storage = CapabilityFilterStorage(storage, self.capabilities)
        if self.item_filter is not None:
            storage = StorageFilterStorage(storage, self.item_filter)
        return storage
