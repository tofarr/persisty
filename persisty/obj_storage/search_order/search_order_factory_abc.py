from abc import ABC, abstractmethod
from enum import Enum

from persisty.storage.search_order import SearchOrderABC, SearchOrder


class SearchOrderFactoryABC(ABC):

    @abstractmethod
    @property
    def field(self) -> Enum:
        """ Get the field to search """

    @abstractmethod
    @property
    def desc(self) -> bool:
        """ Determine if search is in inverse order """

    def to_search_order(self) -> SearchOrderABC:
        """ Convert this search order factory to a search order instance """
        return SearchOrder(
            field=self.field.value,
            desc=self.desc
        )
