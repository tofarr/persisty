from dataclasses import dataclass
from typing import Tuple, Optional

from servey.security.authorization import Authorization

from persisty.security.named_permission import NamedPermission
from persisty.security.store_access import StoreAccess, ALL_ACCESS
from persisty.security.store_security_abc import StoreSecurityABC
from persisty.store.store_abc import StoreABC


@dataclass(frozen=True)
class StoreSecurity(StoreSecurityABC):
    default_access: StoreAccess
    scoped_permissions: Tuple[NamedPermission, ...] = tuple()
    potential_access: StoreAccess = ALL_ACCESS

    def __post_init__(self):
        for scoped_permission in self.scoped_permissions:
            scoped_permission.store_access.validate_against(self.potential_access)

    def get_potential_access(self) -> StoreAccess:
        return self.potential_access

    def get_access(
        self, store_name: str, authorization: Optional[Authorization]
    ) -> StoreAccess:
        if authorization:
            stores_permissions: Optional[Tuple[NamedPermission, ...]] = getattr(
                authorization, "permissions", None
            )
            if stores_permissions:
                for named_permission in stores_permissions:
                    if named_permission.name == store_name:
                        store_access = named_permission.store_access
                        store_access.validate_against(self.potential_access)
                        return store_access
            for scope_permission in self.scoped_permissions:
                if authorization.has_scope(scope_permission.name):
                    return scope_permission.store_access
        return self.default_access

    def get_unsecured(self, store: StoreABC):
        from persisty.security.restrict_access_store import RestrictAccessStore

        return RestrictAccessStore(store, self.potential_access)

    def get_secured(
        self, store: StoreABC, authorization: Optional[Authorization]
    ) -> StoreABC:
        meta = store.get_meta()
        store_access = self.get_access(meta.name, authorization)
        if store_access == ALL_ACCESS:
            return store
        from persisty.security.restrict_access_store import RestrictAccessStore

        return RestrictAccessStore(store, store_access)


UNSECURED = StoreSecurity(ALL_ACCESS, tuple(), ALL_ACCESS)
