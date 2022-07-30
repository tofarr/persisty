from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Any, Set, FrozenSet


class AuthorizationError(Exception):
    pass


@dataclass(frozen=True)
class Authorization:
    user_id: Optional[
        Any
    ]  # Probably a UUID - none implies some sort of headless access
    permissions: FrozenSet[str]
    activate_at: Optional[datetime]
    expire_at: Optional[datetime]

    def is_valid_for_timestamp(self, ts: Optional[datetime] = None):
        activate_at = self.activate_at
        expire_at = self.expire_at
        if not activate_at and not expire_at:
            return True  # No timestamp specified
        if ts is None:
            ts = datetime.now()
        if activate_at and ts < activate_at:
            return False
        if expire_at and ts > expire_at:
            return False
        return True

    def has_permission(self, permission: str):
        has_permission = permission in self.permissions
        return has_permission

    def has_any_permission(self, permissions: Set[str]) -> bool:
        has_any = bool(self.permissions.intersection(permissions))
        return has_any

    def has_all_permissions(self, permissions: Set[str]) -> bool:
        has_all = self.permissions.issuperset(permissions)
        return has_all

    def check_valid_for_timestamp(self, ts: Optional[datetime] = None):
        if not self.is_valid_for_timestamp(ts):
            raise AuthorizationError(f"authorization_expired")

    def check_permission(self, permission: str):
        if not self.has_permission(permission):
            raise AuthorizationError(f"missing:{permission}")

    def check_any_permission(self, permissions: Set[str]):
        if not self.has_any_permission(permissions):
            raise AuthorizationError(f"missing_any:{permissions}")

    def check_all_permissions(self, permissions: Set[str]):
        if not self.has_all_permissions(permissions):
            raise AuthorizationError(
                f"missing_all:{self.permissions.difference(permissions)}"
            )


# A default ROOT permission - rules may be adjusted to remove root access
ROOT = Authorization(None, frozenset(("root",)), None, None)
