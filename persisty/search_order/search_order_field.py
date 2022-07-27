from typing import Any, Tuple, Iterator

from dataclasses import dataclass
from marshy.types import ExternalItemType, ExternalType

from persisty.field.field import Field
from persisty.util.undefined import UNDEFINED


@dataclass(frozen=True)
class SearchOrderField:
    field: str
    desc: bool = False

    def validate_for_fields(self, fields: Tuple[Field, ...]):
        for f in fields:
            if f.name == self.field:
                return
        raise ValueError(f"search_order_invalid:{self.field}")

    def sort(self, items: Iterator[ExternalItemType]) -> Iterator[ExternalItemType]:
        items = sorted(items, key=self.key, reverse=self.desc)
        return items

    def key(self, item: ExternalItemType) -> Any:
        value: ExternalType = item.get(self.field, UNDEFINED)
        if value in (None, UNDEFINED):
            return not self.desc, ""
        else:
            return self.desc, value

    def lt(self, a: ExternalItemType, b: ExternalItemType) -> bool:
        if self.desc:
            return self.key(a) > self.key(b)
        else:
            return self.key(a) < self.key(b)

    def eq(self, a: ExternalItemType, b: ExternalItemType) -> bool:
        return self.key(a) == self.key(b)
