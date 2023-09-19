from abc import abstractmethod, ABC
from dataclasses import dataclass

from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta


@dataclass
class StoreFactoryABC(ABC):
    """
    Factory for store objects which allows defining access control rules for store.
    """

    @abstractmethod
    def create(self, meta: StoreMeta) -> StoreABC:
        """Create a store"""
