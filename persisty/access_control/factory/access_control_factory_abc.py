from abc import abstractmethod, ABC

from persisty.access_control.authorization import Authorization


class AccessControlFactoryABC(ABC):
    priority: int = 100

    @abstractmethod
    def create_access_control(self, storage_name: str, authorization: Authorization):
        """Create an access control based on the authorization given"""
