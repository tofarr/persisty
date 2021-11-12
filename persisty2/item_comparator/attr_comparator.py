from dataclasses import dataclass

from persisty2.item_comparator.item_comparator_abc import ItemComparatorABC, T


@dataclass(frozen=True)
class AttrComparator(ItemComparatorABC[T]):
    attr: str

    def key(self, item: T):
        value = getattr(item, self.attr)
        return value
