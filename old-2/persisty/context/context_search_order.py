from enum import Enum
from typing import Tuple

from persisty.item.item import item
from persisty.search_order.search_order import SearchOrder


class ContextSearchOrderField(Enum):
    NAME = 'name'


@item
class ContextSearchOrder(SearchOrder):
    field_names: Tuple[ContextSearchOrderField, ...]
