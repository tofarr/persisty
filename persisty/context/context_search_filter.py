from typing import Optional

from persisty.item.item import item
from persisty.search_filter.item_search_filter_abc import ItemSearchFilterABC
from persisty.util.undefined import UNDEFINED


@item
class ContextSearchFilter(ItemSearchFilterABC):
    query: Optional[str] = UNDEFINED
