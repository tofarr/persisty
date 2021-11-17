from dataclasses import dataclass
from typing import Iterable, Generic, Union, Sized, List

from persisty.access_control.access_control import NO_ACCESS
from persisty.security.current_user import get_current_user
from persisty.security.role_check import RoleCheck
from persisty.security.role_storage_filter import RoleStorageFilter
from persisty.storage.storage_abc import StorageABC, T
from persisty.storage.wrappers.access_filtered_storage import AccessFilteredStorage
from persisty.storage.wrappers.wrapper_storage_abc import WrapperStorageABC


@dataclass(frozen=True)
class StorageByRole(Generic[T]):
    role_check: RoleCheck
    storage: StorageABC[T]


@dataclass(frozen=True)
class RoleSecuredStorage(WrapperStorageABC[T]):
    role_storages: Union[Iterable[StorageByRole], Sized]
    default_storage: StorageABC[T]

    @property
    def storage(self) -> StorageABC[T]:
        user = get_current_user()
        if user:
            for r in self.role_storages:
                if r.role_check.match(user):
                    return r.storage
        return self.default_storage


def role_secured_storage(storage: StorageABC[T],
                         role_storage_filters: List[RoleStorageFilter[T]],
                         default_storage: StorageABC[T] = None):
    role_storage_filters.sort(key=lambda f: f.capabilities)
    role_storages = tuple(StorageByRole(f.role_check, f.filter_storage(storage)) for f in role_storage_filters)
    if default_storage is None:
        default_storage = AccessFilteredStorage(storage, NO_ACCESS)
    return RoleSecuredStorage(role_storages, default_storage)
