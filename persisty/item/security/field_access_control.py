from dataclasses import dataclass, fields

from persisty.item.security.field_access_control_abc import FieldAccessControlABC
from persisty.security.authorization import Authorization


@dataclass(frozen=True)
class FieldAccessControl(FieldAccessControlABC):
    readable: bool = False
    writable: bool = False

    def is_readable(self, authorization: Authorization) -> bool:
        return self.readable

    def is_writable(self, authorization: Authorization) -> bool:
        return self.writable


ALL_ACCESS = FieldAccessControl(readable=True, writable=True)
READ_ONLY = FieldAccessControl(readable=True)
NO_ACCESS = FieldAccessControl()
