from __future__ import annotations

from typing import Tuple

from marshy import ExternalType

from persisty.storage.field.field import Field
from persisty.storage.search_filter.search_filter_abc import SearchFilterABC
from persisty.util.singleton_abc import SingletonABC


class ExcludeAll(SearchFilterABC, SingletonABC):

    def validate_for_fields(self, fields: Tuple[Field, ...]) -> bool:
        return True

    def match(self, value: ExternalType, fields: Tuple[Field, ...]) -> bool:
        return False


EXCLUDE_ALL = ExcludeAll()
