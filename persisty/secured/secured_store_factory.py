from dataclasses import dataclass

from persisty.secured.access_control_store_factory_abc import AccessControlStoreFactoryABC, T
from persisty.store.restrict_access_store import restrict_access_store
from persisty.store.store_abc import StoreABC
from persisty.store_access import READ_ONLY, StoreAccess, ALL_ACCESS


@dataclass
class SecuredStoreFactory(AccessControlStoreFactoryABC[T]):
    """
    Factory for store objects in the context of servey.
    """
    allow: StoreAccess = ALL_ACCESS
    deny: StoreAccess = READ_ONLY

    def allow_store(self, store: StoreABC[T]) -> StoreABC[T]:
        return restrict_access_store(store, self.allow)

    def deny_store(self, store: StoreABC[T]) -> StoreABC[T]:
        return restrict_access_store(store, self.deny)
