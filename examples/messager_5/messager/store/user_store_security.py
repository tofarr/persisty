from persisty.security.store_access import StoreAccess, ALL_ACCESS
from persisty.security.store_security_abc import StoreSecurityABC
from persisty.store.store_abc import StoreABC
from servey.security.authorization import Authorization


class UserStoreSecurity(StoreSecurityABC):
    def get_store_access(self) -> StoreAccess:
        return ALL_ACCESS

    def get_secured(self, store: StoreABC, authorization: Authorization) -> StoreABC:
        from messager.store.secured_user_store import SecuredUserStore

        return SecuredUserStore(store, authorization)
