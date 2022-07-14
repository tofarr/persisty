import importlib
import pkgutil
from abc import ABC, abstractmethod
from dataclasses import fields
from typing import Optional, Type, Union

from persisty.access_control.access_control_abc import AccessControlABC
from persisty.attr.attr import attr_from_field
from persisty.errors import PersistyError
from persisty.key_config.attr_key_config import UuidKeyConfig
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
            key_config=UuidKeyConfig(attr='name'),
            access_control=self.access_control
        )
        return meta

    @property
    def item_type(self) -> Type[StorageMeta]:
        return StorageMeta

    @abstractmethod
    def get_storage(self, key: Union[str, Type]) -> Optional[StorageABC]:
        """ Get the storage with the key given. """

    @abstractmethod
    def register_storage(self, storage: StorageABC):
        """
        Register the storage object given. (This is for the current runtime only, and will not be
        present in a different app instance - it is intended for static setup on application start)
        """


_default_context = None
CONFIG_MODULE_PREFIX = 'persisty_config_'


def get_default_storage_context() -> StorageContextABC:
    global _default_context
    if not _default_context:
        _default_context = new_default_storage_context()
    return _default_context


def new_default_storage_context() -> StorageContextABC:
    module_info = (m for m in pkgutil.iter_modules() if m.name.startswith(CONFIG_MODULE_PREFIX))
    modules = [importlib.import_module(m.name) for m in module_info]
    modules.sort(key=lambda m: m.priority, reverse=True)
    storage_context = None
    for module in modules:
        if hasattr(module, 'create_storage_context'):
            storage_context = module.create_storage_context()
            break
    if storage_context is None:
        raise PersistyError('no_storage_context')
    modules.sort(key=lambda m: m.priority)
    for module in modules:
        if hasattr(module, 'configure_storage_context'):
            module.configure_storage_context(storage_context)
    return storage_context
