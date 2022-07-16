from abc import abstractmethod, ABC
from typing import Optional

from persisty.security.authorization import Authorization
from persisty.storage.storage_abc import StorageABC


class StorageFactoryABC(ABC):

    @abstractmethod
    @property
    def priority(self) -> int:
        """ Priority for this factory """

    @abstractmethod
    def get_storage(self, name: str, authorization: Authorization) -> Optional[StorageABC]:
        """ Get storage by name """
