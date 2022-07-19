from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional

from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_field import SearchOrderField


class SearchOrderFactoryABC(ABC):
    @property
    @abstractmethod
    def field(self) -> Optional[Enum]:
        """Get the field to search"""

    @property
    @abstractmethod
    def desc(self) -> bool:
        """Determine if search is in inverse order"""

    def to_search_order(self) -> Optional[SearchOrder]:
        """Convert this search order factory to a search order instance"""
        if not self.field:
            return None
        return SearchOrder((SearchOrderField(self.field.value, self.desc),))
