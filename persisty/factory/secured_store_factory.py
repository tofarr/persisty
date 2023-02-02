from dataclasses import dataclass

from persisty.factory.access_control_store_factory_abc import AccessControlStoreFactoryABC
from persisty.store.restrict_access_store import restrict_access_store
from persisty.store.store_abc import StoreABC, T
from persisty.store_access import READ_ONLY, StoreAccess, ALL_ACCESS


@dataclass
class SecuredStoreFactory(AccessControlStoreFactoryABC[T]):
    """
    Factory for store objects in the context of servey.
    """

    allow: StoreAccess = ALL_ACCESS
    deny: StoreAccess = READ_ONLY

    def allow_store(self) -> StoreABC[T]:
        return restrict_access_store(self.store, self.allow)

    def deny_store(self) -> StoreABC[T]:
        return restrict_access_store(self.store, self.deny)
