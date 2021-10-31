from dataclasses import dataclass
from typing import Iterable, Generic, Union, Sized, List

from persisty.capabilities import NO_CAPABILITIES
from persisty.store.capability_filter_store import CapabilityFilterStore
from persisty.secure.current_user import get_current_user
from persisty.secure.role_check import RoleCheck
from persisty.secure.role_store_filter import RoleStoreFilter
from persisty.store.store_abc import StoreABC, T
from persisty.store.wrapper_store_abc import WrapperStoreABC


@dataclass(frozen=True)
class RoleStore(Generic[T]):
    role_check: RoleCheck
    store: StoreABC[T]


@dataclass(frozen=True)
class RoleSecuredStore(WrapperStoreABC[T]):
    role_stores: Union[Iterable[RoleStore], Sized]
    default_store: StoreABC[T]

    @property
    def store(self) -> StoreABC[T]:
        user = get_current_user()
        if user:
            for r in self.role_stores:
                if r.role_check.match(user):
                    return r.store
        return self.default_store


def role_secured_store(store: StoreABC[T],
                       role_store_filters: List[RoleStoreFilter[T]],
                       default_store: StoreABC[T] = None):
    role_store_filters.sort(key=lambda f: f.capabilities)
    role_stores = tuple(RoleStore(f.role_check, f.filter_store(store)) for f in role_store_filters)
    if default_store is None:
        default_store = CapabilityFilterStore(store, NO_CAPABILITIES)
    return RoleSecuredStore(role_stores, default_store)
