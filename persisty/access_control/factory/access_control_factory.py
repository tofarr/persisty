from dataclasses import dataclass

from persisty.access_control.constants import NO_ACCESS, READ_ONLY
from persisty.access_control.access_control_abc import AccessControlABC
from persisty.access_control.authorization import Authorization
from persisty.access_control.factory.access_control_factory_abc import AccessControlFactoryABC


@dataclass(frozen=True)
class AccessControlFactory(AccessControlFactoryABC):
    access_control: AccessControlABC = NO_ACCESS
    priority: int = 100

    def create_access_control(self, storage_name: str, authorization: Authorization):
        return self.access_control


NO_ACCESS_FACTORY = AccessControlFactory()
READ_ONLY_ACCESS_FACTORY = AccessControlFactory(READ_ONLY)
