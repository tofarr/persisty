from dataclasses import dataclass

from servey.security.authorization import Authorization

from persisty.access_control.access_control_abc import AccessControlABC
from persisty.access_control.constants import ALL_ACCESS
from persisty.access_control.factory.access_control_factory_abc import (
    AccessControlFactoryABC,
)


@dataclass(frozen=True)
class ScopeAccessControlFactory(AccessControlFactoryABC):
    scope: str
    access_control: AccessControlABC = ALL_ACCESS

    def create_access_control(self, authorization: Authorization):
        if authorization.has_scope(self.scope):
            return self.access_control
