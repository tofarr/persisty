from typing import Callable, Any, Tuple
from dataclasses import dataclass

from persisty.item.field import Field
from persisty.search_filter.item_filter.item_filter_abc import ItemFilterABC
from persisty.search_filter.item_filter.item_filter_factory_abc import ItemFilterFactoryABC


@dataclass(frozen=True)
class SuffixFactory(ItemFilterFactoryABC):
    suffix: str
    constructor: Callable[[Field, Any], ItemFilterABC]

    def create(self, item_fields: Tuple[Field, ...], filter_name: str, filter_value: Any) -> Optional[ItemFilterABC]:
        if not filter_name.endswith(self.suffix):
            return None
        field_name = filter_name[:-len(self.suffix)]
        field = next((f for f in item_fields if f.name == field_name), None)
        if field:
            return self.constructor(field, filter_value)
