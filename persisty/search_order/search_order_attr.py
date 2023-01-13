from typing import Any, Tuple, Iterator, Generic, TypeVar

from dataclasses import dataclass

from persisty.attr.attr import Attr
from persisty.util.undefined import UNDEFINED

T = TypeVar("T")


@dataclass(frozen=True)
class SearchOrderAttr(Generic[T]):
    attr: str
    desc: bool = False

    def validate_for_attrs(self, attrs: Tuple[Attr, ...]):
        for f in attrs:
            if f.name == self.attr:
                return
        raise ValueError(f"search_order_invalid:{self.attr}")

    def sort(self, items: Iterator[T]) -> Iterator[T]:
        items = sorted(items, key=self.key, reverse=self.desc)
        return items

    def key(self, item: T) -> Any:
        value = getattr(item, self.attr, UNDEFINED)
        if value in (None, UNDEFINED):
            return not self.desc, ""
        else:
            return self.desc, value

    def lt(self, a: T, b: T) -> bool:
        if self.desc:
            return self.key(a) > self.key(b)
        else:
            return self.key(a) < self.key(b)

    def eq(self, a: T, b: T) -> bool:
        return self.key(a) == self.key(b)
