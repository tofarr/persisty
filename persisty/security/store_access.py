from __future__ import annotations

import dataclasses
from dataclasses import dataclass, fields
from typing import Tuple

from persisty.attr.attr import Attr
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.util import UNDEFINED


@dataclass(frozen=True)
class StoreAccess:
    """
    Summary of the level of access currently available in a store
    """

    create_filter: SearchFilterABC = INCLUDE_ALL
    read_filter: SearchFilterABC = INCLUDE_ALL
    update_filter: SearchFilterABC = INCLUDE_ALL
    delete_filter: SearchFilterABC = INCLUDE_ALL
    searchable: bool = True

    def __and__(self, other: StoreAccess) -> StoreAccess:
        result = StoreAccess(
            **{
                f.name: getattr(self, f.name) & getattr(other, f.name)
                for f in fields(StoreAccess)
            }
        )
        return result

    def __or__(self, other: StoreAccess) -> StoreAccess:
        result = StoreAccess(
            **{
                f.name: getattr(self, f.name) | getattr(other, f.name)
                for f in fields(StoreAccess)
            }
        )
        return result

    @property
    def editable(self):
        return (
            self.create_filter != EXCLUDE_ALL
            or self.update_filter != EXCLUDE_ALL
            or self.delete_filter != EXCLUDE_ALL
        )

    def item_creatable(self, item, attrs: Tuple[Attr, ...]):
        return self.create_filter.match(item, attrs)

    def item_readable(self, item, attrs: Tuple[Attr, ...]):
        return self.read_filter.match(item, attrs)

    def item_updatable(self, item, updates, attrs: Tuple[Attr, ...]):
        if self.update_filter == INCLUDE_ALL:
            return True
        if not self.update_filter.match(item, attrs):
            return False
        new_item = dataclasses.replace(
            item, **{k: v for k, v in dataclasses.asdict(updates) if v is not UNDEFINED}
        )
        return self.update_filter.match(new_item, attrs)

    def item_deletable(self, item, attrs: Tuple[Attr, ...]):
        return self.delete_filter.match(item, attrs)


NO_ACCESS = StoreAccess(
    create_filter=EXCLUDE_ALL,
    read_filter=EXCLUDE_ALL,
    update_filter=EXCLUDE_ALL,
    delete_filter=EXCLUDE_ALL,
    searchable=False,
)
ALL_ACCESS = StoreAccess(
    create_filter=INCLUDE_ALL,
    read_filter=INCLUDE_ALL,
    update_filter=INCLUDE_ALL,
    delete_filter=INCLUDE_ALL,
)
READ_ONLY = StoreAccess(
    create_filter=EXCLUDE_ALL,
    read_filter=INCLUDE_ALL,
    update_filter=EXCLUDE_ALL,
    delete_filter=EXCLUDE_ALL,
)
NO_UPDATES = StoreAccess(update_filter=EXCLUDE_ALL)
