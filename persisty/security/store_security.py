from dataclasses import dataclass
from typing import Tuple, Optional

from servey.security.authorization import Authorization

from persisty.security.named_permission import NamedPermission
from persisty.security.store_access import StoreAccess, ALL_ACCESS, NO_ACCESS
from persisty.security.store_security_abc import StoreSecurityABC, T
from persisty.store.store_abc import StoreABC


@dataclass(frozen=True)
class StoreSecurity(StoreSecurityABC[T]):
    default_access: StoreAccess
    scoped_permissions: Tuple[NamedPermission, ...] = tuple()
    api_access: StoreAccess = ALL_ACCESS

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
                        return store_access
            for scope_permission in self.scoped_permissions:
                if authorization.has_scope(scope_permission.name):
                    return scope_permission.store_access
        return self.default_access

    def get_secured(
        self, store: StoreABC, authorization: Optional[Authorization]
    ) -> StoreABC:
        meta = store.get_meta()
        store_access = self.get_access(meta.name, authorization)
        if store_access == ALL_ACCESS:
            return store
        from persisty.security.restrict_access_store import RestrictAccessStore

        return RestrictAccessStore(store, store_access & meta.store_access)

    def get_api(self, store: StoreABC) -> StoreABC:
        if self.api_access == ALL_ACCESS:
            return store
        from persisty.security.restrict_access_store import RestrictAccessStore

        return RestrictAccessStore(
            store, self.api_access & store.get_meta().store_access
        )


UNSECURED = StoreSecurity(ALL_ACCESS, tuple(), ALL_ACCESS)
INTERNAL_ONLY = StoreSecurity(NO_ACCESS, tuple(), NO_ACCESS)
