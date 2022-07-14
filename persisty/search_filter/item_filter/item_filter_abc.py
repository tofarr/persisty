from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar('T')


class ItemFilterABC(ABC, Generic[T]):

    @abstractmethod
    def match(self, item: T) -> bool:
        """ Determine if this op matches the value given """
