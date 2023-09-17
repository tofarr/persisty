from dataclasses import dataclass
from typing import Optional, Tuple

from servey.security.authorization import Authorization

from persisty.security.permission import Permission


@dataclass(frozen=True)
class PermissionAuthorization(Authorization):
    permissions: Optional[Tuple[Permission, ...]]
