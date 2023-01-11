from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Generic

from servey.security.authorization import Authorization

from aaaa.store.store_abc import StoreABC, T
from aaaa.store_meta import StoreMeta


@dataclass
class SecuredStoreFactoryABC(Generic[T], ABC):
    """
    Factory for store objects which allows defining access control rules for store.
    """

    @abstractmethod
    def get_store_meta(self) -> StoreMeta:
        """ Get the meta for the store """

    @abstractmethod
    def create(self, authorization: Optional[Authorization]) -> Optional[StoreABC[T]]:
        """ Create a new store instance """
