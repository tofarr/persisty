from dataclasses import dataclass, fields
from typing import List

from marshy.marshaller.marshaller_abc import MarshallerABC, T


@dataclass(frozen=True)
class Capabilities:
    """
    Capabilities object that shows exactly what a repository is capable of
    """
    create: bool = False
    create_with_key: bool = False
    read: bool = False
    update: bool = False
    destroy: bool = False
    bulk_edit: bool = False
    search: bool = False

    def __eq__(self, other):
        different_fields = (f for f in fields(Capabilities) if getattr(self, f.name) != getattr(other, f.name))
        return next(different_fields, None) is None

    def __and__(self, other):
        if self == other:
            return self
        kwargs = {f.name: getattr(self, f.name) and getattr(other, f.name) for f in fields(Capabilities)}
        return Capabilities(**kwargs)

    def __or__(self, other):
        if self == other:
            return self
        kwargs = {f.name: getattr(self, f.name) or getattr(other, f.name) for f in fields(Capabilities)}
        return Capabilities(**kwargs)

    def __sub__(self, other):
        if self == NO_CAPABILITIES or other == NO_CAPABILITIES or self == other:
            return NO_CAPABILITIES
        kwargs = {f.name: getattr(self, f.name) and not getattr(other, f.name) for f in fields(Capabilities)}
        return Capabilities(**kwargs)

    def __xor__(self, other):
        kwargs = {f.name: getattr(self, f.name) != getattr(other, f.name) for f in fields(Capabilities)}
        return Capabilities(**kwargs)


ALL_CAPABILITIES = Capabilities(**{f.name: True for f in fields(Capabilities)})
NO_CAPABILITIES = Capabilities(**{f.name: False for f in fields(Capabilities)})


class CapabilitiesMarshaller(MarshallerABC):

    def load(self, item: List[str]) -> T:
        return Capabilities(**{c: True for c in item})

    def dump(self, item: T) -> List[str]:
        return [f.name for f in fields(Capabilities) if getattr(self, f.name) is True]
