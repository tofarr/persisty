import dataclasses
from dataclasses import dataclass

from marshy.factory.optional_marshaller_factory import get_optional_type

from persisty2.item_filter.item_filter_abc import ItemFilterABC, T


@dataclass(frozen=True)
class QueryFilter(ItemFilterABC[T]):
    query: str

    def __post_init__(self):
        object.__setattr__(self, 'query', self.query.lower())

    def match(self, item: T) -> bool:
        for f in dataclasses.fields(item):
            if f.name.endswith('id'):
                continue
            field_type = get_optional_type(f.type) or f.type
            if field_type is str:
                value = getattr(item, f.name)
                if value is not None and self.query in value.lower():
                    return True
        return False
