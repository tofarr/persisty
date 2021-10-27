from dataclasses import dataclass
from typing import Iterable, Callable, Any

from persisty.capabilities import NO_CAPABILITIES
from persisty.store.capability_filter_store import CapabilityFilterStore
from persisty.store.search_filter_store import SearchFilterStore
from persisty.store.secure.current_user import get_current_user
from persisty.store.secure.role_check import RoleCheck
from persisty.store.store_abc import StoreABC, T
from persisty.store.wrapper_store_abc import WrapperStoreABC


@dataclass(frozen=True)
class RoleFilter:
    role_check: RoleCheck
    item_filter: Callable[[T], bool]
    search_filter_factory: Callable[[Any], Any] = None


@dataclass(frozen=True)
class RoleBasedSecuredStore(WrapperStoreABC[T]):
    store: StoreABC[T]
    role_filters: Iterable[RoleFilter]

    @property
    def store(self) -> StoreABC[T]:
        user = get_current_user()
        for r in self.role_filters:
            if r.role_check.match(user):
                return SearchFilterStore(self.store, r.item_filter, r.search_filter_factory)
        return CapabilityFilterStore(self.store, NO_CAPABILITIES)  # Lock out
