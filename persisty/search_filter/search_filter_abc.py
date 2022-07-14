from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Tuple

from persisty.item.field import Field

T = TypeVar('T')


class SearchFilterABC(ABC, Generic[T]):

    @abstractmethod
    def match(self, fields: Tuple[Field, ...], value: T) -> bool:
        """ Determine if the item given matches this filter """
