from __future__ import annotations

from typing import Tuple, Optional, Any, TYPE_CHECKING
from uuid import uuid4

from marshy import ExternalType

from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.util.singleton_abc import SingletonABC

if TYPE_CHECKING:
    from persisty.storage.field.field import Field


class ExcludeAll(SearchFilterABC, SingletonABC):
    def validate_for_fields(self, fields: Tuple[Field, ...]) -> bool:
        return True

    def match(self, value: ExternalType, fields: Tuple[Field, ...]) -> bool:
        return False

    def build_filter_expression(
        self, fields: Tuple[Field, ...]
    ) -> Tuple[Optional[Any], bool]:
        """This should be caught as it means no query should run"""
        from boto3.dynamodb.conditions import Attr

        return Attr(str(uuid4())).eq(1), True


EXCLUDE_ALL = ExcludeAll()
