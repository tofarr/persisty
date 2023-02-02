from abc import abstractmethod
from dataclasses import dataclass
from typing import Optional

from servey.security.access_control.access_control_abc import AccessControlABC
from servey.security.access_control.scope_access_control import ScopeAccessControl
from servey.security.authorization import Authorization

from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.store.store_abc import StoreABC, T
from persisty.store_meta import StoreMeta


@dataclass
class AccessControlStoreFactoryABC(StoreFactoryABC[T]):
    """
    Factory for store objects based on an access control
    """

    store: StoreABC[T]
    access_control: AccessControlABC = ScopeAccessControl("root")

    def get_meta(self) -> StoreMeta:
        return self.store.get_meta()

    def create(self, authorization: Optional[Authorization]) -> Optional[StoreABC[T]]:
        if self.access_control.is_executable(authorization):
            store = self.allow_store()
        else:
            store = self.deny_store()
        return store

    @abstractmethod
    def allow_store(self) -> StoreABC[T]:
        """Filter when access is allowed"""

    @abstractmethod
    def deny_store(self) -> StoreABC[T]:
        """Filter when access is denied"""
