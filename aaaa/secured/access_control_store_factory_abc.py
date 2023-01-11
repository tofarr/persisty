from abc import abstractmethod
from dataclasses import dataclass
from typing import Optional

from servey.security.access_control.access_control_abc import AccessControlABC
from servey.security.access_control.scope_access_control import ScopeAccessControl
from servey.security.authorization import Authorization

from aaaa.secured.secured_store_factory_abc import SecuredStoreFactoryABC, T
from aaaa.store.store_abc import StoreABC
from aaaa.store.store_factory_abc import StoreFactoryABC
from aaaa.store_meta import StoreMeta


@dataclass
class AccessControlStoreFactoryABC(SecuredStoreFactoryABC[T]):
    """
    Factory for store objects based on an access control
    """
    store_factory: StoreFactoryABC[T]
    access_control: AccessControlABC = ScopeAccessControl('root')

    def get_store_meta(self) -> StoreMeta:
        return self.store_factory.get_meta()

    def create(self, authorization: Optional[Authorization]) -> Optional[StoreABC[T]]:
        store = self.store_factory.create()
        if self.access_control.is_executable(authorization):
            store = self.allow_store(store)
        else:
            store = self.deny_store(store)
        return store

    @abstractmethod
    def allow_store(self, store: StoreABC[T]) -> StoreABC[T]:
        """ Filter when access is allowed"""

    @abstractmethod
    def deny_store(self, store: StoreABC[T]) -> StoreABC[T]:
        """ Filter when access is denied"""
