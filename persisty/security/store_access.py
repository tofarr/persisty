from __future__ import annotations
from dataclasses import dataclass, fields

from persisty.errors import PersistyError
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC


@dataclass(frozen=True)
class StoreAccess:
    creatable: bool = True
    readable: bool = True
    updatable: bool = True
    deletable: bool = True
    searchable: bool = True
    create_filter: SearchFilterABC = INCLUDE_ALL
    read_filter: SearchFilterABC = INCLUDE_ALL
    update_filter: SearchFilterABC = INCLUDE_ALL
    delete_filter: SearchFilterABC = INCLUDE_ALL

    def __and__(self, other: StoreAccess) -> StoreAccess:
        result = StoreAccess(
            **{
                f.name: getattr(self, f.name) and getattr(other, f.name)
                for f in fields(StoreAccess)
            }
        )
        return result

    def __or__(self, other: StoreAccess) -> StoreAccess:
        result = StoreAccess(
            **{
                f.name: getattr(self, f.name) or getattr(other, f.name)
                for f in fields(StoreAccess)
            }
        )
        return result

    @property
    def editable(self):
        return self.creatable or self.updatable or self.deletable

    def validate_against(self, store_access: StoreAccess):
        for permission in (
            "creatable",
            "readable",
            "updatable",
            "deletable",
            "searchable",
        ):
            if getattr(self, permission) and not getattr(store_access, permission):
                raise PersistyError("invalid_permission")


NO_ACCESS = StoreAccess(**{f.name: False for f in fields(StoreAccess)})
ALL_ACCESS = StoreAccess()
READ_ONLY = StoreAccess(creatable=False, updatable=False, deletable=False)
