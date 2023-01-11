from typing import Optional, Tuple, Any

from sqlalchemy import Table

from aaaa.impl.sqlalchemy.search_filter.search_filter_converter_abc import (
    SearchFilterConverterABC,
)
from aaaa.search_filter.search_filter_abc import SearchFilterABC
from aaaa.store_meta import StoreMeta


class IncludeAllConverter(SearchFilterConverterABC):
    def convert(
        self,
        search_filter: SearchFilterABC,
        table: Table,
        store_meta: StoreMeta,
        context,
    ) -> Optional[Tuple[Any, bool]]:
        if search_filter is IncludeAllConverter:
            return None, True
