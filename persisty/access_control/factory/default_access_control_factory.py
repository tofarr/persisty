from dataclasses import dataclass
from typing import Optional

from persisty.access_control.access_control_abc import AccessControlABC
from persisty.access_control.authorization import Authorization
from persisty.access_control.constants import ALL_ACCESS, READ_ONLY
from persisty.access_control.factory.access_control_factory_abc import (
    AccessControlFactoryABC,
)


@dataclass(frozen=True)
class DefaultAccessControlFactory(AccessControlFactoryABC):
    access_control: AccessControlABC = READ_ONLY

    def create_access_control(self, authorization: Authorization):
        return self.access_control
