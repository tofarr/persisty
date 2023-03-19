from __future__ import annotations

from typing import Tuple, Optional, Any, TYPE_CHECKING
from uuid import uuid4

from servey.util.singleton_abc import SingletonABC

from persisty.search_filter.search_filter_abc import SearchFilterABC, T

if TYPE_CHECKING:
    from persisty.attr.attr import Attr


class ExcludeAll(SearchFilterABC[T], SingletonABC):
    def lock_attrs(self, attrs: Tuple[Attr, ...]) -> ExcludeAll:
        return self

    def match(self, value: T, attrs: Tuple[Attr, ...]) -> bool:
        return False

    def build_filter_expression(
        self, attrs: Tuple[Attr, ...]
    ) -> Tuple[Optional[Any], bool]:
        """This should be caught as it means no query should run"""
        from boto3.dynamodb.conditions import Attr

        return Attr(str(uuid4())).eq(1), True


EXCLUDE_ALL = ExcludeAll()
