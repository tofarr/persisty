from abc import ABC, abstractmethod
from typing import Optional
from persisty.storage import StorageABC
from persisty.storage.storage_meta import StorageMeta


class RegistryABC(ABC, StorageABC[StorageMeta]):

    @abstractmethod
    def get_storage(self, name: str) -> Optional[StorageABC]:
        """ Get storage """

    SHOULD THIS PASS IN THE Authorization Object?
    SHOULD WE HAVE AN AUTHORIZATION OBJECT?
    HOW SHOULD AUTH WORK WITH THE ACCESS CONTROL - A GLOBAL ITEM FEELS WRONG

    what about get_storage(self, name: str, auth: Authorization)
    what about the access control users the authorization to check?