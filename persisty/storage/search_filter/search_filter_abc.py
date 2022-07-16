from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Tuple

from marshy import ExternalType

from persisty.storage.field.field import Field


class SearchFilterABC(ABC):

    @abstractmethod
    def validate_for_fields(self, fields: Tuple[Field, ...]) -> bool:
        """ Validate that this search_filter is applicable for the fields given """

    @abstractmethod
    def match(self, item: ExternalType, fields: Tuple[Field, ...]) -> bool:
        """ Determine if the item given matches this search_filter """

    def __and__(self, obj_filter: SearchFilterABC) -> SearchFilterABC:
        if not isinstance(obj_filter, SearchFilterABC):
            raise TypeError(f'SearchFilterABC:{obj_filter}')
        from persisty.storage.search_filter.and_filter import And
        return And((self, obj_filter))

    def __or__(self, obj_filter: SearchFilterABC) -> SearchFilterABC:
        if not isinstance(obj_filter, SearchFilterABC):
            raise TypeError(f'SearchFilterABC:{obj_filter}')
        from persisty.storage.search_filter.or_filter import Or
        return Or((self, obj_filter))

    def __invert__(self) -> SearchFilterABC:
        from persisty.storage.search_filter.not_filter import Not
        return Not(self)
