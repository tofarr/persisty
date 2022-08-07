from typing import Optional, Tuple, Any

from sqlalchemy import Table

from persisty.impl.sqlalchemy.search_filter.search_filter_converter_abc import (
    SearchFilterConverterABC,
)
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.storage_meta import StorageMeta


class IncludeAllConverter(SearchFilterConverterABC):
    def convert(
        self,
        search_filter: SearchFilterABC,
        table: Table,
        storage_meta: StorageMeta,
        context,
    ) -> Optional[Tuple[Any, bool]]:
        if search_filter is IncludeAllConverter:
            return None, True
