from __future__ import annotations
from dataclasses import dataclass, fields


@dataclass(frozen=True)
class StoreAccess:
    creatable: bool = True
    readable: bool = True
    updatable: bool = True
    deletable: bool = True
    searchable: bool = True

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


NO_ACCESS = StoreAccess(**{f.name: False for f in fields(StoreAccess)})
ALL_ACCESS = StoreAccess()
READ_ONLY = StoreAccess(creatable=False, updatable=False, deletable=False)
