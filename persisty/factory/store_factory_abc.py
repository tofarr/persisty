from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Generic

from servey.security.authorization import Authorization

from persisty.store.store_abc import StoreABC, T
from persisty.store_meta import StoreMeta


@dataclass
class StoreFactoryABC(Generic[T], ABC):
    """
    Factory for store objects which allows defining access control rules for store.
    """

    @abstractmethod
    def get_meta(self) -> StoreMeta:
        """Get the meta for the store"""

    @abstractmethod
    def create(self, authorization: Optional[Authorization]) -> Optional[StoreABC[T]]:
        """Create a new store instance"""
