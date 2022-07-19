from abc import abstractmethod
from typing import Generic, Optional, TypeVar

from persisty.search_filter.search_filter_factory_abc import SearchFilterFactoryABC
from persisty.search_order.search_order_factory_abc import SearchOrderFactoryABC

T = TypeVar("T")
F = TypeVar("F", bound=SearchFilterFactoryABC)
S = TypeVar("S", bound=SearchOrderFactoryABC)
C = TypeVar("C")
U = TypeVar("U")


class ObjAccessControlABC(Generic[T, F, C, U]):
    @abstractmethod
    def is_creatable(self, create_input: C) -> bool:
        """Determine if the stored given may be created"""

    @abstractmethod
    def is_readable(self, item: T) -> bool:
        """Determine if the stored given should be returned for read"""

    @abstractmethod
    def is_updatable(self, old_item: T, updates: U) -> bool:
        """Determine if the stored given may be updated"""

    @abstractmethod
    def is_deletable(self, item: T) -> bool:
        """Determine if the stored given may be updated"""

    @abstractmethod
    def is_searchable(self) -> bool:
        """Determine if the store is searchable"""

    @abstractmethod
    def transform_search_filter(
        self, search_filter_factory: Optional[F]
    ) -> Optional[F]:
        """
        Transform the search filter given so that it conforms to the access control constraints.
        """
