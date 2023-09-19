from abc import ABC, abstractmethod
from typing import Type, Optional

from servey.security.authorization import Authorization

from persisty.security.store_access import StoreAccess

_StoreABC = "persisty.store.store_abc.StoreABC"


class StoreSecurityABC(ABC):
    @abstractmethod
    def get_potential_access(self) -> StoreAccess:
        """Get the potential (max) access"""

    def get_unsecured(self, store: _StoreABC) -> _StoreABC:
        """Wrap a store if required to yield a version that conforms to unsecured mode"""
        return store

    @abstractmethod
    def get_secured(
        self, store: _StoreABC, authorization: Optional[Authorization]
    ) -> _StoreABC:
        """
        Get the access for a store given the authorization
        """
