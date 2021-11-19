from typing import TypeVar, Optional

from marshy import get_default_context

from persisty.entity.entity_abc import EntityABC
from persisty.entity.entity_context import EntityContext, get_default_entity_context
from persisty.server.handlers.app_handler import AppHandler
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
from persisty.server.handlers.handler_abc import HandlerABC
from persisty.storage.dynamic_storage_abc import DynamicStorageABC
from persisty.storage.in_mem.in_mem_dynamic_storage import InMemDynamicStorage
from persisty.storage.storage_context import StorageContext

priority = 0
E = TypeVar('E', bound=EntityABC)


def create_dynamic_storage() -> DynamicStorageABC:
    return InMemDynamicStorage()


def configure_storage_context(storage_context: StorageContext):
    pass


def configure_entities(entity_context: EntityContext):
    pass


def configure_server_handler(handler: Optional[HandlerABC] = None) -> HandlerABC:
    handlers = []
    if isinstance(handler, AppHandler):
        handlers.extend(handler.handlers)
    elif handler:
        handlers.append(handler)
    entity_context = get_default_entity_context()
    marshaller_context = get_default_context()
    handlers.append(EntitiesMetaHandler(entity_context, marshaller_context))
    handlers.append(EntityCountHandler(entity_context, marshaller_context))
    handlers.append(EntityCreateHandler(entity_context, marshaller_context))
    handlers.append(EntityDestroyHandler(entity_context, marshaller_context))
    handlers.append(EntityEditAllHandler(entity_context, marshaller_context))
    handlers.append(EntityMetaHandler(entity_context, marshaller_context))
    handlers.append(EntityPagedSearchHandler(entity_context, marshaller_context))
    handlers.append(EntityReadAllHandler(entity_context, marshaller_context))
    handlers.append(EntityReadHandler(entity_context, marshaller_context))
    handlers.append(EntityUpdateHandler(entity_context, marshaller_context))
    handlers.sort(key=lambda h: h.priority, reverse=True)
    handler = AppHandler(handlers)
    return handler
