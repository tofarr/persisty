from abc import ABC, abstractmethod
from typing import Optional

from servey.security.authorization import Authorization

from persisty.data_storage.data_storage_abc import DataStorageABC


class DataStorageFactoryABC(ABC):

    @abstractmethod
    def get_name(self) -> str:
        """ Get a name for this storage """

    @abstractmethod
    def get_description(self) -> Optional[str]:
        """ Get a description of this storage """

    @abstractmethod
    def create(self, authorization: Authorization) -> DataStorageABC:
        """ Create a new data storage instance given the authorization provided """
