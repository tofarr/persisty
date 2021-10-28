from dataclasses import dataclass
from typing import Iterable

from persisty.capabilities import Capabilities, NO_CAPABILITIES
from persisty.store.capability_filter_store import CapabilityFilterStore
from persisty.store.secure.current_user import get_current_user
from persisty.store.secure.role_check import RoleCheck
from persisty.store.store_abc import StoreABC, T
from persisty.store.wrapper_store_abc import WrapperStoreABC


@dataclass(frozen=True)
class RoleCapabilities:
    role_check: RoleCheck
    capabilities: Capabilities


@dataclass(frozen=True)
class RoleBasedCapabilityStore(WrapperStoreABC[T]):
    store: StoreABC[T]
    role_capabilities: Iterable[RoleCapabilities]

    @property
    def store(self) -> StoreABC[T]:
        user = get_current_user()
        for r in self.role_capabilities:
            if r.role_check.match(user):
                return CapabilityFilterStore(self.store, r.capabilities)
        return CapabilityFilterStore(self.store, NO_CAPABILITIES)
