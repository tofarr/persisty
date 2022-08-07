from abc import ABC, abstractmethod
from typing import Optional, Tuple, Any

from sqlalchemy import Table

from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.storage_meta import StorageMeta


class SearchFilterConverterABC(ABC):
    priority: int = 100

    @abstractmethod
    def convert(
        self,
        search_filter: SearchFilterABC,
        table: Table,
        storage_meta: StorageMeta,
        context,
    ) -> Optional[Tuple[Any, bool]]:
        """
        Convert to a sqlalchemy where clause. Return null if this is not possible. Returned boolean
        indicates whether the resulting clause fully handles this filter
        """
