from __future__ import annotations
from dataclasses import dataclass, fields


@dataclass(frozen=True)
class StorageAccess:
    creatable: bool = True
    readable: bool = True
    updatable: bool = True
    deletable: bool = True
    searchable: bool = True

    def __and__(self, other: StorageAccess) -> StorageAccess:
        result = StorageAccess(**{
            f.name: getattr(self, f.name) and getattr(other, f.name)
            for f in fields(StorageAccess)
        })
        return result

    def __or__(self, other: StorageAccess) -> StorageAccess:
        result = StorageAccess(**{
            f.name: getattr(self, f.name) or getattr(other, f.name)
            for f in fields(StorageAccess)
        })
        return result

    @property
    def editable(self):
        return self.creatable or self.updatable or self.deletable


NO_ACCESS = StorageAccess(**{f.name: False for f in fields(StorageAccess)})
ALL_ACCESS = StorageAccess()
READ_ONLY = StorageAccess(creatable=False, updatable=False, deletable=False)
