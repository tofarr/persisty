import dataclasses
from dataclasses import dataclass
from typing import Tuple, Optional

from servey.security.authorization import Authorization

from persisty.security.named_permission import NamedPermission
from persisty.security.store_access import StoreAccess, ALL_ACCESS, NO_ACCESS
from persisty.security.store_security_abc import StoreSecurityABC, T
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta


@dataclass(frozen=True)
class StoreSecurity(StoreSecurityABC[T]):
    default_access: StoreAccess
    scoped_permissions: Tuple[NamedPermission, ...] = tuple()
    api_access: StoreAccess = ALL_ACCESS

    def get_access(
        self, store_meta: StoreMeta, authorization: Optional[Authorization]
    ) -> StoreAccess:
        if authorization:
            stores_permissions: Optional[Tuple[NamedPermission, ...]] = getattr(
                authorization, "permissions", None
            )
            if stores_permissions:
                store_name = store_meta.name
                for named_permission in stores_permissions:
                    if named_permission.name == store_name:
                        store_access = named_permission.to_store_access(store_meta)
                        return store_access
            for scope_permission in self.scoped_permissions:
                if authorization.has_scope(scope_permission.name):
                    result = scope_permission.to_store_access(store_meta)
                    return result
        return self.default_access

    def get_secured(
        self, store: StoreABC, authorization: Optional[Authorization]
    ) -> StoreABC:
        meta = store.get_meta()
        store_access = self.get_access(meta, authorization)
        if store_access == ALL_ACCESS:
            return store
        from persisty.security.restrict_access_store import RestrictAccessStore

        return RestrictAccessStore(store, store_access & meta.store_access)

    def get_api_meta(self, store_meta: StoreMeta) -> StoreMeta:
        if self.api_access == ALL_ACCESS:
            return store_meta
        result = dataclasses.replace(
            store_meta, store_access=self.api_access & store_meta.store_access
        )
        return result


UNSECURED = StoreSecurity(ALL_ACCESS, tuple(), ALL_ACCESS)
INTERNAL_ONLY = StoreSecurity(NO_ACCESS, tuple(), NO_ACCESS)
