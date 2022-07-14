from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar('T')


class SearchFilterABC(ABC, Generic[T]):

    @abstractmethod
    def match(self, value: T) -> bool:
        """ Determine if the item given matches this filter """
