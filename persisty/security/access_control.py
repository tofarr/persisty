from dataclasses import dataclass, fields

from persisty.security.access_control_abc import AccessControlABC
from persisty.security.authorization import Authorization


@dataclass(frozen=True)
class AccessControl(AccessControlABC):
    meta_accessible: bool = False
    creatable: bool = False
    readable: bool = False
    updatable: bool = False
    deletable: bool = False
    searchable: bool = False

    def is_meta_accessible(self, authorization: Authorization):
        return self.meta_accessible

    def is_creatable(self, authorization: Authorization) -> bool:
        return self.creatable

    def is_readable(self, authorization: Authorization) -> bool:
        return self.readable

    def is_updatable(self, authorization: Authorization) -> bool:
        return self.updatable

    def is_deletable(self, authorization: Authorization) -> bool:
        return self.deletable

    def is_searchable(self, authorization: Authorization) -> bool:
        return self.searchable

    def __and__(self, other):
        if self == other:
            return self
        kwargs = {f.name: getattr(self, f.name) and getattr(other, f.name) for f in fields(AccessControl)}
        return AccessControl(**kwargs)

    def __or__(self, other):
        if self == other:
            return self
        kwargs = {f.name: getattr(self, f.name) or getattr(other, f.name) for f in fields(AccessControl)}
        return AccessControl(**kwargs)

    def __add__(self, other):
        return self.__or__(other)

    def __sub__(self, other):
        if other == NO_ACCESS:
            return self
        if self == NO_ACCESS or self == other:
            return NO_ACCESS
        kwargs = {f.name: getattr(self, f.name) and not getattr(other, f.name) for f in fields(AccessControl)}
        return AccessControl(**kwargs)

    def __lt__(self, other):
        eq = True
        for f in fields(AccessControl):
            v = getattr(self, f.name)
            other_v = getattr(other, f.name)
            if v != other_v:
                if v:
                    return False
                else:
                    eq = False
        return not eq


ALL_ACCESS = AccessControl(**{f.name: True for f in fields(AccessControl)})
READ_ONLY = AccessControl(meta_accessible=True, readable=True, searchable=True)
NO_ACCESS = AccessControl()
