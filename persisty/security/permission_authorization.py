from dataclasses import dataclass
from typing import Optional, Tuple

from servey.security.authorization import Authorization

from persisty.security.named_permission import NamedPermission


@dataclass(frozen=True)
class PermissionAuthorization(Authorization):
    stores_permissions: Optional[Tuple[NamedPermission, ...]]
