from abc import abstractmethod, ABC
from typing import Optional

from persisty.context.obj_storage_meta import MetaStorageABC
from persisty.context.obj_timestamp import TimestampStorageABC
from persisty.access_control.authorization import Authorization
from persisty.storage.storage_abc import StorageABC


class PersistyContextABC(ABC):

    @abstractmethod
    def get_storage(self, name: str, authorization: Authorization) -> Optional[StorageABC]:
        """ Get the storage with the name given """

    @abstractmethod
    def get_meta_storage(self, authorization: Authorization) -> MetaStorageABC:
        """ Get the storage for meta. Used to grab info about this context. """

    # noinspection PyMethodMayBeStatic, PyUnusedLocal
    def get_timestamp_storage(self, authorization: Authorization) -> Optional[TimestampStorageABC]:
        """
        Get a read only storage object which contains timestamp info for other storage objects.
        Useful for caching.
        Not all implementations support this.
        """
        return None
