from dataclasses import dataclass
from typing import Generic, Optional

from persisty.capabilities import Capabilities, ALL_CAPABILITIES
from persisty2.item_filter.item_filter_abc import ItemFilterABC, T
from persisty.store.capability_filter_store import CapabilityFilterStore
from persisty.store.search_filter_store import SearchFilterStore
from persisty.secure.role_check import RoleCheck
from persisty.store.store_abc import StoreABC


@dataclass(frozen=True)
class RoleStoreFilter(Generic[T]):
    role_check: RoleCheck
    capabilities: Capabilities = ALL_CAPABILITIES
    item_filter: Optional[ItemFilterABC[T]] = None

    def filter_store(self, store: StoreABC[T]):
        if self.capabilities != ALL_CAPABILITIES:
            store = CapabilityFilterStore(store, self.capabilities)
        if self.item_filter is not None:
            store = SearchFilterStore(store, self.item_filter)
        return store
