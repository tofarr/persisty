from __future__ import annotations

from typing import Tuple, Optional, Any

from marshy import ExternalType

from persisty.field.field import Field
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.util.singleton_abc import SingletonABC


class IncludeAll(SearchFilterABC, SingletonABC):
    def validate_for_fields(self, fields: Tuple[Field, ...]) -> bool:
        return True

    def match(self, item: ExternalType, fields: Tuple[Field, ...]) -> bool:
        return True

    def build_filter_expression(
        self, fields: Tuple[Field, ...]
    ) -> Tuple[Optional[Any], bool]:
        return None, True


INCLUDE_ALL = IncludeAll()
