import importlib
import os
from abc import abstractmethod
from typing import Union, Type, Iterator

from persisty.obj_graph.old_entity_abc import EntityABC
from persisty.storage.storage_abc import StorageABC, T
from persisty.storage.storage_meta import StorageMeta


class PersistyContextABC:

    @abstractmethod
    def get_storage(self, name: Union[str, T]) -> StorageABC[T]:
        """
        Get the named storage object given
        """

    @abstractmethod
    def get_meta_storage(self) -> StorageABC[StorageMeta]:
        """
        Get the storage object used to administer storages. Depending on the implementation and access, this may be used
        to create new storages or delete existing storages.
        """

    @abstractmethod
    def get_entities(self) -> Iterator[Type[EntityABC]]:
        """
        Get the available entities
        """


_default_context = None
PERSISTY_CONTEXT = 'PERSISTY_CONTEXT'


def get_default_persisty_context() -> PersistyContextABC:
    global _default_context
    if not _default_context:
        # Set up the default_context based on an environment variable
        import_name = os.environ.get(PERSISTY_CONTEXT,
                                     'persisty.storage.in_mem.in_mem_storage_context.InMemStorageContext')
        import_path = import_name.split('.')
        import_module = '.'.join(import_path[:-1])
        imported_module = importlib.import_module(import_module)
        context_fn = getattr(imported_module, import_path[-1])
        _default_context = context_fn()
    return _default_context
