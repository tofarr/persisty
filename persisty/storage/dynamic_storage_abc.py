from abc import ABC, abstractmethod

from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta


class DynamicStorageABC(StorageABC[StorageMeta], ABC):

    """
    Storage objects can be defined by code in the program, or linked dynamically based on remote resources
    (For example in a table in SQL or DynamoDB)

    This class represents the situation where the storage resource is defined on the fly at runtime based on what is
    available in the remote resource. StorageMeta is used as a proxy for the object itself and the get_storage
    method is used to translate meta into a storage object, which may then be cached.
    """

    @abstractmethod
    def get_storage(self, name: str):
        """ Get the storage object for the named value given """
