from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Any

T = TypeVar('T')


class ItemComparatorABC(ABC, Generic[T]):
    """ Object for filtering items """

    @abstractmethod
    def key(self, item: T) -> Any:
        """ Determine if the stored given matches this search_filter. """
