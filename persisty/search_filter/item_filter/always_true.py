from __future__ import annotations
from typing import Any

from persisty.search_filter.item_filter.item_filter_abc import ItemFilterABC
from persisty.util.singleton_abc import SingletonABC


class AlwaysTrue(ItemFilterABC, SingletonABC):

    def match(self, value: Any) -> bool:
        return True


ALWAYS_TRUE = AlwaysTrue()
