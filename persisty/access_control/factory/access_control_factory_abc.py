from abc import abstractmethod, ABC
from typing import Optional

from persisty.access_control.access_control import AccessControl
from persisty.access_control.authorization import Authorization


class AccessControlFactoryABC(ABC):
    @abstractmethod
    def create_access_control(
        self, authorization: Authorization
    ) -> Optional[AccessControl]:
        """Create an access control based on the authorization given"""
