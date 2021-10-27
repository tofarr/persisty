from dataclasses import dataclass
from typing import Union, Iterable, Sized


@dataclass(frozen=True)
class RoleCheck:
    roles: Union[Iterable[str], Sized]
    all: bool = False

    def match(self, user):
        roles = getattr(user, 'roles')
        if self.all:
            for role in self.roles:
                if role not in roles:
                    return True
            return False
        else:
            for role in self.roles:
                if role in roles:
                    return True
            return False
