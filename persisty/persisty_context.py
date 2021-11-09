import importlib
import os
from typing import Union, Type, TypeVar, Iterator, Optional

from marshy import get_default_context
from marshy.marshaller_context import MarshallerContext

from persisty.errors import PersistyError
from persisty.obj_graph.entity_abc import EntityABC
from persisty.server.handlers.handler_abc import HandlerABC
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

    def get_request_handler(self, marshaller_context: Optional[MarshallerContext] = None) -> HandlerABC:
        if not marshaller_context:
            marshaller_context = get_default_context()
        from persisty.server.handlers.entities_meta_handler import EntitiesMetaHandler
        from persisty.server.handlers.entity_count_handler import EntityCountHandler
        from persisty.server.handlers.entity_create_handler import EntityCreateHandler
        from persisty.server.handlers.entity_destroy_handler import EntityDestroyHandler
        from persisty.server.handlers.entity_edit_all_handler import EntityEditAllHandler
        from persisty.server.handlers.entity_meta_handler import EntityMetaHandler
        from persisty.server.handlers.entity_paged_search_handler import EntityPagedSearchHandler
        from persisty.server.handlers.entity_read_all_handler import EntityReadAllHandler
        from persisty.server.handlers.entity_read_handler import EntityReadHandler
        from persisty.server.handlers.entity_update_handler import EntityUpdateHandler
        from persisty.server.handlers.app_handler import AppHandler
        handlers = [
            EntitiesMetaHandler(self, marshaller_context),
            EntityCountHandler(self, marshaller_context),
            EntityCreateHandler(self, marshaller_context),
            EntityDestroyHandler(self, marshaller_context),
            EntityEditAllHandler(self, marshaller_context),
            EntityMetaHandler(self, marshaller_context),
            EntityPagedSearchHandler(self, marshaller_context),
            EntityReadAllHandler(self, marshaller_context),
            EntityReadHandler(self, marshaller_context),
            EntityUpdateHandler(self, marshaller_context)
        ]
        handlers.sort()
        return AppHandler(handlers)


_default_context = None
PERSISTY_CONTEXT = 'PERSISTY_CONTEXT'


def get_default_persisty_context() -> PersistyContext:
    global _default_context
    if not _default_context:
        # Set up the default_context based on an environment variable
        import_name = os.environ.get(PERSISTY_CONTEXT, f'{__name__}.{PersistyContext.__name__}')
        import_path = import_name.split('.')
        import_module = '.'.join(import_path[:-1])
        imported_module = importlib.import_module(import_module)
        context_fn = getattr(imported_module, import_path[-1])
        _default_context = context_fn()

        marshy_context = get_default_context()
        from persisty.marshaller.edit_marshaller_factory import EditMarshallerFactory
        marshy_context.register_factory(EditMarshallerFactory())
        from persisty.marshaller.page_marshaller_factory import PageMarshallerFactory
        marshy_context.register_factory(PageMarshallerFactory())

        from persisty.store.in_mem_store import in_mem_store
        from persisty.store.schema_store import schema_store
        from persisty.store.logging_store import LoggingStore
        from tests.fixtures.items import Band, Member
        from tests.fixtures.entities import BandEntity, MemberEntity
        _default_context.register_store(
            schema_store(
                LoggingStore(
                    in_mem_store(Band)
                )
            )
        )
        _default_context.register_store(
            schema_store(
                LoggingStore(
                    in_mem_store(Member)
                )
            )
        )
        _default_context.register_entity(BandEntity)
        _default_context.register_entity(MemberEntity)
        from tests.fixtures.data import setup_bands
        setup_bands(_default_context.get_store(Band))
        from tests.fixtures.data import setup_members
        setup_members(_default_context.get_store(Member))

    return _default_context
