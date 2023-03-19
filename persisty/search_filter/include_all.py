from __future__ import annotations

from typing import Tuple, Optional, Any

from servey.util.singleton_abc import SingletonABC

from persisty.attr.attr import Attr
from persisty.search_filter.search_filter_abc import SearchFilterABC, T


class IncludeAll(SearchFilterABC[T], SingletonABC):
    def lock_attrs(self, attrs: Tuple[Attr, ...]) -> IncludeAll:
        return self

    def match(self, item: T, attrs: Tuple[Attr, ...]) -> bool:
        return True

    def build_filter_expression(
        self, attrs: Tuple[Attr, ...]
    ) -> Tuple[Optional[Any], bool]:
        return None, True


INCLUDE_ALL = IncludeAll()
