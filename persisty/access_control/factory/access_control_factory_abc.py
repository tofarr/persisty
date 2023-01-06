from abc import abstractmethod, ABC
from typing import Optional

from servey.security.authorization import Authorization

from persisty.access_control.access_control import AccessControl


class AccessControlFactoryABC(ABC):
    @abstractmethod
    def create_access_control(
        self, authorization: Authorization
    ) -> Optional[AccessControl]:
        """Create an access control based on the authorization given"""
