from dataclasses import dataclass, fields

from persisty2.access_control.access_control_abc import AccessControlABC


@dataclass(frozen=True)
class AccessControl(AccessControlABC):
    is_creatable: bool = False
    is_readable: bool = False
    is_updatable: bool = False
    is_destroyable: bool = False
    is_searchable: bool = False

    def __eq__(self, other):
        if not isinstance(other, AccessControl):
            return False
        different_fields = (f for f in fields(AccessControl) if getattr(self, f.name) != getattr(other, f.name))
        return next(different_fields, None) is None

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
READ_ONLY = AccessControl(is_readable=True, is_searchable=True)
NO_ACCESS = AccessControl()
