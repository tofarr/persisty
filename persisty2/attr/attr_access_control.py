from dataclasses import dataclass, fields

from persisty2.attr.attr_access_control_abc import AttrAccessControlABC
from persisty2.attr.attr_mode import AttrMode


@dataclass(frozen=True)
class AttrAccessControl(AttrAccessControlABC):
    create_mode: AttrMode = AttrMode.OPTIONAL
    update_mode: AttrMode = AttrMode.OPTIONAL
    read_mode: AttrMode = AttrMode.OPTIONAL
    search_mode: AttrMode = AttrMode.OPTIONAL

    def __eq__(self, other):
        if not isinstance(other, AttrAccessControl):
            return False
        different_fields = (f for f in fields(AttrAccessControl) if getattr(self, f.name) != getattr(other, f.name))
        return next(different_fields, None) is None


OPTIONAL = AttrAccessControl()
