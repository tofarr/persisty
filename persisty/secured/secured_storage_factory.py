from dataclasses import dataclass

from persisty.secured.access_control_storage_factory_abc import AccessControlStorageFactoryABC
from persisty.storage.restrict_access_storage import restrict_access_storage
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_access import READ_ONLY, StorageAccess, ALL_ACCESS


@dataclass
class SecuredStorageFactory(AccessControlStorageFactoryABC):
    """
    Factory for storage objects in the context of servey.
    """
    allow: StorageAccess = ALL_ACCESS
    deny: StorageAccess = READ_ONLY

    def allow_storage(self, storage: StorageABC) -> StorageABC:
        return restrict_access_storage(storage, self.allow)

    def deny_storage(self, storage: StorageABC) -> StorageABC:
        return restrict_access_storage(storage, self.deny)
