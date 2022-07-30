from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Tuple, Optional, Any, TYPE_CHECKING

from marshy import ExternalType


if TYPE_CHECKING:
    from persisty.field.field import Field


class SearchFilterABC(ABC):
    @abstractmethod
    def validate_for_fields(self, fields: Tuple[Field, ...]) -> bool:
        """Validate that this search_filter is applicable for the fields given"""

    @abstractmethod
    def match(self, item: ExternalType, fields: Tuple[Field, ...]) -> bool:
        """Determine if the stored given matches this search_filter"""

    def __and__(self, obj_filter: SearchFilterABC) -> SearchFilterABC:
        if not isinstance(obj_filter, SearchFilterABC):
            raise TypeError(f"SearchFilterABC:{obj_filter}")
        from persisty.search_filter.and_filter import And

        return And((self, obj_filter))

    def __or__(self, obj_filter: SearchFilterABC) -> SearchFilterABC:
        if not isinstance(obj_filter, SearchFilterABC):
            raise TypeError(f"SearchFilterABC:{obj_filter}")
        from persisty.search_filter.or_filter import Or

        return Or((self, obj_filter))

    def __invert__(self) -> SearchFilterABC:
        from persisty.search_filter.not_filter import Not

        return Not(self)

    def build_filter_expression(
        self, fields: Tuple[Field, ...]
    ) -> Tuple[Optional[Any], bool]:
        """
        Build a dynamodb filter expression from this search filter if possible, and return it. Return True if this
        filter was completely represented by the condition, False otherwise"""
        return None, False
