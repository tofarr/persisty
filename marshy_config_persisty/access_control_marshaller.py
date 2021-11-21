from dataclasses import fields
from typing import List

from marshy.marshaller.marshaller_abc import MarshallerABC, T

from persisty.access_control.access_control import AccessControl


class AccessControlMarshaller(MarshallerABC):

    def __init__(self):
        super().__init__(AccessControl)

    def load(self, item: List[str]) -> T:
        kwargs = {f.name: (f.name in item) for f in fields(AccessControl)}
        loaded = AccessControl(**kwargs)
        return loaded

    def dump(self, item: T) -> List[str]:
        dumped = [f.name for f in fields(AccessControl) if getattr(item, f.name)]
        return dumped
