from dataclasses import dataclass
from typing import Union, Iterable, Sized

ROLES = 'roles'


@dataclass(frozen=True)
class RoleCheck:
    """
    Checker for whether a user has certain roles (Represented by strings). User objects are expected to have a
    `roles` property.
    """
    roles: Union[Iterable[str], Sized]
    all: bool = False

    def match(self, user):
        roles = getattr(user, ROLES)
        if self.all:
            for role in self.roles:
                if role not in roles:
                    return False
            return True
        else:
            for role in self.roles:
                if role in roles:
                    return True
            return False
