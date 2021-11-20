from abc import ABC, abstractmethod
from dataclasses import fields
from typing import Optional, Type

from persisty.access_control.access_control_abc import AccessControlABC
from persisty.attr.attr import attr_from_field
from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta


class StorageContextABC(StorageABC[StorageMeta], ABC):

    @property
    @abstractmethod
    def access_control(self) -> AccessControlABC:
        pass

    @property
    def meta(self) -> StorageMeta:
        meta = StorageMeta(
            name=StorageMeta.name,
            attrs=tuple(attr_from_field(f) for f in fields(StorageMeta)),
            key_config=AttrKeyConfig(attr='name'),
            access_control=self.access_control
        )
        return meta

    @property
    def item_type(self) -> Type[StorageMeta]:
        return StorageMeta

    @abstractmethod
    def get_storage(self, key: str) -> Optional[StorageABC]:
        """ Get the storage with the key given. """

    @abstractmethod
    def register_storage(self, storage: StorageABC):
        """
        Register the storage object given. (This is for the current runtime only, and will not be
        present in a different app instance - it is intended for static setup on application start)
        """
