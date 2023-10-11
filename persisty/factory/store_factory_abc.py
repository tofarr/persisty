from abc import abstractmethod, ABC

from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta


class StoreFactoryABC(ABC):
    """
    Factory for store objects which allows defining access control rules for store.
    """

    @abstractmethod
    def create(self, store_meta: StoreMeta) -> StoreABC:
        """Create a store"""
