import importlib
import pkgutil
from dataclasses import dataclass, field
from typing import Iterator, Set, Optional, TypeVar, Type, Dict

from persisty.entity.entity_abc import EntityABC
from persisty.storage.storage_abc import StorageABC, T
from persisty.storage.storage_context_abc import StorageContextABC, get_default_storage_context

E = TypeVar('E', bound=EntityABC)


@dataclass(frozen=True)
class EntityContext:
    storage_context: StorageContextABC
    _entities: Dict[str, Type[E]] = field(default_factory=dict)

    def get_storage(self, type_: T) -> StorageABC[T]:
        storage = self.storage_context.get_storage(type_)
        return storage

    def get_entity_names(self) -> Iterator[str]:
        return iter(self._entities)

    def get_entities(self) -> Iterator[Type[E]]:
        return iter(self._entities.values())

    def register_entity(self, entity_type: Type[E]):
        name = entity_type.__name__
        self._entities[name] = entity_type

    def deregister_entity(self, name: str):
        del self._entities[name]

    def has_entity(self, name: str):
        return name in self._entities

    def get_entity(self, name: str):
        return self._entities[name]


_default_context = None
CONFIG_MODULE_PREFIX = 'persisty_config_'


def get_default_entity_context() -> EntityContext:
    global _default_context
    if not _default_context:
        _default_context = new_default_entity_context()
    return _default_context


def new_default_entity_context(storage_context: Optional[StorageContextABC] = None) -> EntityContext:
    if storage_context is None:
        storage_context = get_default_storage_context()
    entity_context = EntityContext(storage_context)
    module_info = (m for m in pkgutil.iter_modules() if m.name.startswith(CONFIG_MODULE_PREFIX))
    modules = [importlib.import_module(m.name) for m in module_info]
    modules.sort(key=lambda m: m.priority)
    for module in modules:
        if hasattr(module, 'configure_entities'):
            module.configure_entities(entity_context)
    return entity_context
