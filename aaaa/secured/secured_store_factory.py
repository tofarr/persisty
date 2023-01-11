from dataclasses import dataclass

from aaaa.secured.access_control_store_factory_abc import AccessControlStoreFactoryABC, T
from aaaa.store.restrict_access_store import restrict_access_store
from aaaa.store.store_abc import StoreABC
from aaaa.store_access import READ_ONLY, StoreAccess, ALL_ACCESS


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
