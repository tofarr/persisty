import importlib
import pkgutil
from dataclasses import dataclass, field
from typing import TypeVar, Dict, Optional, Union, Type

from persisty.errors import PersistyError
from persisty.storage.dynamic_storage_abc import DynamicStorageABC
from persisty.storage.storage_abc import StorageABC

T = TypeVar('T')


@dataclass(frozen=True)
class StorageContext:
    dynamic_storage: Optional[DynamicStorageABC] = None
    _storage_by_name: Dict[str, StorageABC] = field(default_factory=dict)

    def register_storage(self, storage: StorageABC[T]):
        name = storage.meta.name
        if name in self._storage_by_name:
            raise PersistyError(f'storage_already_exists:{name}')
        self._storage_by_name[name] = storage

    def unregister_storage(self, name: str):
        del self._storage_by_name[name]

    def get_storage(self, name: Union[str, Type[T]]) -> StorageABC[T]:
        if isinstance(name, type):
            name = name.__name__
        storage = self._storage_by_name.get(name)
        if not storage and self.dynamic_storage:
            storage = self.dynamic_storage.get_storage(name)
            self._storage_by_name[name] = storage
        if not storage:
            raise PersistyError(f'unknown_storage:{name}')
        return storage


_default_context = None
CONFIG_MODULE_PREFIX = 'persisty_config_'
CREATE_FN = 'create_dynamic_storage'
CONFIGURE_FN = 'configure_storage_context'


def get_default_storage_context() -> StorageContext:
    global _default_context
    if not _default_context:
        _default_context = new_default_storage_context()
    return _default_context


def new_default_storage_context() -> StorageContext:
    module_info = (m for m in pkgutil.iter_modules() if m.name.startswith(CONFIG_MODULE_PREFIX))
    modules = [importlib.import_module(m.name) for m in module_info]

    # Use the highest priority module to create the context
    modules.sort(key=lambda m: m.priority, reverse=True)
    module = next(m for m in modules if hasattr(m, CREATE_FN))
    # noinspection PyUnresolvedReferences
    storage_context = module.create_dynamic_storage()

    # Run the configure methods in ascending priority order - so higher priority ones run later and can override
    modules.sort(key=lambda m: m.priority)
    for m in modules:
        if hasattr(m, CONFIGURE_FN):
            # noinspection PyUnresolvedReferences
            m.configure_storage_context(storage_context)

    return storage_context
