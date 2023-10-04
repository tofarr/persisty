from abc import ABC, abstractmethod
from typing import Optional, Generic, TypeVar

from servey.security.authorization import Authorization

_StoreABC = "persisty.store.store_abc.StoreABC"
T = TypeVar("T")


class StoreSecurityABC(ABC, Generic[T]):
    """Object which can be used to wrap a store to add security constraints"""

    @abstractmethod
    def get_secured(
        self, store: _StoreABC, authorization: Optional[Authorization]
    ) -> _StoreABC:
        """
        Get the access for a store given the authorization
        """

    @abstractmethod
    def get_api(self, store: _StoreABC) -> _StoreABC:
        """
        Get the api access - the max potential access for this store for apis.
        Used for generating actions and metadata.
        """

