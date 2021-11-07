from typing import Optional

from persisty.obj_graph.entity_abc import EntityABC
from persisty.server.handlers.entity.entity_handler_abc import EntityHandlerABC
from persisty.server.request import Request
from persisty.server.response import Response


class EntityReadHandler(EntityHandlerABC):

    def match(self, request: Request) -> bool:
        return request.method == 'GET' and self.get_key(request) is not None

    def handle_request(self, request: Request) -> Response:
        entity_type = self.get_entity_type(request)
        key = self.get_key(request)
        if entity_type is None or key is None:
            return Response(404)

        selections = self.get_selections(request)
        entity: EntityABC = entity_type.read(key, selections)
        if entity is None:
            return Response(404)

        cache_header = entity.get_cache_header()
        response_headers = cache_header.get_cache_control_headers()

        if not self.is_modified(request, cache_header):
            return Response(304, response_headers)

        content = self.marshaller_context.dump(entity)
        return Response(200, response_headers, content)

    @staticmethod
    def get_key(request: Request) -> Optional[str]:
        num_parts = len(request.path)
        if num_parts == 1:
            return request.params.get('key')
        elif num_parts == 2:
            return request.path[1]
