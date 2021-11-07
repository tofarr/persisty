import importlib
import os
from typing import Union, Type, TypeVar, Iterator

from persisty.errors import PersistyError
from persisty.obj_graph.entity_abc import EntityABC
from persisty.store.store_abc import StoreABC

T = TypeVar('T')


class PersistyContext:

    def __init__(self):
        self._stores_by_name = {}
        self._entities_by_name = {}

    def register_store(self, store: StoreABC[T]):
        self._stores_by_name[store.name] = store

    def get_store(self, key: Union[str, Type[T]]) -> StoreABC[T]:
        if not isinstance(key, str):
            key = key.__name__
        store = self._stores_by_name.get(key)
        if store is None:
            raise PersistyError(f'missing_store:{key}')
        return store

    def get_stores(self) -> Iterator[StoreABC]:
        return iter(self._stores_by_name.values())

    def register_entity(self, entity_type: Type[EntityABC]):
        self._entities_by_name[entity_type.get_name()] = entity_type

    def has_entity(self, entity_name):
        return entity_name in self._entities_by_name

    def get_entity(self, entity_name: str) -> EntityABC:
        return self._entities_by_name[entity_name]

    def get_entities(self) -> Iterator[EntityABC]:
        return iter(self._entities_by_name.values())


_default_context = None
PERSISTY_CONTEXT = 'PERSISTY_CONTEXT'


def get_default_persisty_context() -> PersistyContext:
    global _default_context
    if not _default_context:
        # Set up the default_context based on an environment variable
        import_name = os.environ.get(PERSISTY_CONTEXT, PersistyContext.__name__)
        import_path = import_name.split('.')
        import_module = '.'.join(import_path[:-1])
        imported_module = importlib.import_module(import_module)
        context_fn = getattr(imported_module, import_path[-1])
        _default_context = context_fn()
    return _default_context
