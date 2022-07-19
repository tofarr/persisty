from abc import abstractmethod, ABC

from persisty.access_control.authorization import Authorization


class AccessControlFactoryABC(ABC):

    @abstractmethod
    def priority(self) -> int:
        """ Get the priority for this factory"""

    @abstractmethod
    def create_access_control(self, storage_name: str, authorization: Authorization):
        """ Create an access control based on the authorization given """
