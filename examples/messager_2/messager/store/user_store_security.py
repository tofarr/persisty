from typing import Optional

from persisty.security.store_access import StoreAccess, ALL_ACCESS
from persisty.security.store_security_abc import StoreSecurityABC, T
from persisty.store.store_abc import StoreABC
from servey.security.authorization import Authorization

from persisty.store_meta import StoreMeta


class UserStoreSecurity(StoreSecurityABC[T]):
    def get_store_access(self) -> StoreAccess:
        return ALL_ACCESS

    def get_secured(self, store: StoreABC, authorization: Authorization) -> StoreABC:
        from messager.store.secured_user_store import SecuredUserStore

        return SecuredUserStore(store, authorization)

    def is_item_updatable(self, store_meta: StoreMeta, item: T, updates: T, authorization: Optional[Authorization]) -> bool:
        if authorization.has_scope("admin"):
            if item.id == authorization.subject_id and item.admin is False:
                return False  # Can't remove admin permission from self
        elif item.id != authorization.subject_id or item.admin is True:
            return False  # Can't edit others or add admin permission to self
        return True

    def is_item_deletable(self, store_meta: StoreMeta, item: T, authorization: Optional[Authorization]) -> bool:
        if (
            not authorization.has_scope("admin")
            or item.id == authorization.subject_id
        ):
            return False
        return True
