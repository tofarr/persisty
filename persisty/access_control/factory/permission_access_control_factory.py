from dataclasses import dataclass
from typing import Optional

from persisty.access_control.access_control_abc import AccessControlABC
from persisty.access_control.authorization import Authorization
from persisty.access_control.constants import ALL_ACCESS
from persisty.access_control.factory.access_control_factory_abc import (
    AccessControlFactoryABC,
)


@dataclass(frozen=True)
class PermissionAccessControlFactory(AccessControlFactoryABC):
    permission: str
    access_control: AccessControlABC = ALL_ACCESS

    def create_access_control(self, authorization: Authorization):
        if authorization.has_permission(self.permission):
            return self.access_control
