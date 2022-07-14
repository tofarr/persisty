from abc import ABC, abstractmethod
from typing import Any, Tuple, Optional

from persisty.item.field import Field
from persisty.search_filter.item_filter.item_filter_abc import ItemFilterABC


class ItemFilterFactoryABC(ABC):

    @abstractmethod
    def create(self, item_fields: Tuple[Field, ...], filter_name: str, filter_value: Any) -> Optional[ItemFilterABC]:
        """ create and return a filter if we can, otherwise return None """
