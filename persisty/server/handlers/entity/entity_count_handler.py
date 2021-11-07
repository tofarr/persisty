from dataclasses import dataclass
from typing import Optional, Type

from persisty.obj_graph.entity_abc import EntityABC
from persisty.search_filter import SearchFilter, search_filter_from_dataclass
from persisty.server.handlers.entity.entity_handler_abc import EntityHandlerABC
from persisty.server.request import Request
from persisty.server.response import Response


@dataclass
class EntityCountHandler(EntityHandlerABC):
    max_limit: 100

    def match(self, request: Request) -> bool:
        return request.method == 'GET' and len(request.path) == 1 and self.is_param_true(request, 'count')

    def handle_request(self, request: Request) -> Response:
        entity_type = self.get_entity_type(request)
        if entity_type is None:
            return Response(404)

        search_filter = self.get_search_filter(request, entity_type)
        count = entity_type.count(search_filter.item_filter)

        return Response(200, content=count)

    def get_search_filter(self, request: Request, entity_type: Type[EntityABC]) -> Optional[SearchFilter]:
        if entity_type.__filter_class__ is not None:
            filter_obj = self.marshaller_context.load(entity_type.__filter_class__, request.params)
            search_filter = search_filter_from_dataclass(filter_obj, entity_type.get_store().item_type)
            return search_filter
