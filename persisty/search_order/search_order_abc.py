from abc import ABC, abstractmethod
from typing import Generic, Any, TypeVar

T = TypeVar('T')


class SearchOrderABC(ABC, Generic[T]):
    """ General object for sorting items """

    @abstractmethod
    def key(self, item: T) -> Any:
        """ Determine if the item given matches this filter. """
