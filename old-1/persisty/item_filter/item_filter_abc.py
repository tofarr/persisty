from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar('T')


class ItemFilterABC(ABC, Generic[T]):
    """ Object for filtering items """

    @abstractmethod
    def match(self, item: T) -> bool:
        """ Determine if the item given matches this filter. """

    def __and__(self, other):
        from persisty.item_filter.and_filter import AndFilter
        filters = [self]
        if isinstance(other, AndFilter):
            filters.extend(other.filters)
        else:
            filters.append(other)
        return AndFilter(tuple(filters))
