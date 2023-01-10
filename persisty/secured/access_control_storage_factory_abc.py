from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from servey.security.access_control.access_control_abc import AccessControlABC
from servey.security.access_control.scope_access_control import ScopeAccessControl
from servey.security.authorization import Authorization

from persisty.secured.secured_storage_factory_abc import SecuredStorageFactoryABC
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_factory_abc import StorageFactoryABC
from persisty.storage.storage_meta import StorageMeta


@dataclass
class AccessControlStorageFactoryABC(SecuredStorageFactoryABC):
    """
    Factory for storage objects based on an access control
    """
    storage_factory: StorageFactoryABC
    access_control: AccessControlABC = ScopeAccessControl('root')

    def get_storage_meta(self) -> StorageMeta:
        return self.storage_factory.get_storage_meta()

    def create(self, authorization: Optional[Authorization]) -> Optional[StorageABC]:
        storage = self.storage_factory.create()
        if self.access_control.is_executable(authorization):
            storage = self.allow_storage(storage)
        else:
            storage = self.deny_storage(storage)
        return storage

    @abstractmethod
    def allow_storage(self, storage: StorageABC) -> StorageABC:
        """ Filter when access is allowed"""

    @abstractmethod
    def deny_storage(self, storage: StorageABC) -> StorageABC:
        """ Filter when access is denied"""
