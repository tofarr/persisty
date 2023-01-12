from abc import ABC, abstractmethod
from typing import Optional, Generic, TypeVar

from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta

T = TypeVar('T')


class StoreFactoryABC(Generic[T], ABC):
    @abstractmethod
    def get_meta(self) -> StoreMeta:
        """Get the store meta"""

    @abstractmethod
    def create(self) -> Optional[StoreABC[T]]:
        """Create a new store object with the authorization given"""
