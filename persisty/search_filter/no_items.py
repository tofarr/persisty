from typing import Any

from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.util.singleton_abc import SingletonABC


class NoItems(SingletonABC, SearchFilterABC[Any]):

    def match(self, value: Any) -> bool:
        return False


NO_ITEMS = NoItems()
