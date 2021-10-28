from dataclasses import dataclass
from typing import Iterable, Generic, Union, Sized, List

from persisty.capabilities import NO_CAPABILITIES
from persisty.store.capability_filter_store import CapabilityFilterStore
from persisty.store.search_filter_store import SearchFilterStore
from persisty.store.secure.current_user import get_current_user
from persisty.store.secure.role_check import RoleCheck
from persisty.store.secure.role_store_filter import RoleStoreFilter
from persisty.store.store_abc import StoreABC, T
from persisty.store.wrapper_store_abc import WrapperStoreABC


@dataclass(frozen=True)
class RoleStore(Generic[T]):
    role_check: RoleCheck
    store: StoreABC[T]


@dataclass(frozen=True)
class RoleSecuredStore(WrapperStoreABC[T]):
    role_stores: Union[Iterable[RoleStore], Sized]

    @property
    def store(self) -> StoreABC[T]:
        user = get_current_user()
        for r in self.role_stores:
            if r.role_check.match(user):
                return SearchFilterStore(self.store, r.item_filter, r.search_filter_factory)
        return CapabilityFilterStore(self.store, NO_CAPABILITIES)  # Lock out


def role_secured_store(store: StoreABC[T], role_store_filters: List[RoleStoreFilter[T]]):
    role_stores = tuple(f.filter_store(store) for f in role_store_filters)
    return RoleSecuredStore(role_stores)
