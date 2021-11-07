from persisty.server.handlers.entity.entity_handler_abc import EntityHandlerABC
from persisty.server.request import Request
from persisty.server.response import Response


class EntityUpdateHandler(EntityHandlerABC):

    def match(self, request: Request) -> bool:
        return len(request.path) == 2 and request.method in ['POST', 'PUT', 'PATCH']

    def handle_request(self, request: Request) -> Response:
        entity_type = self.get_entity_type(request)
        if entity_type is None:
            return Response(404)
        previous_entity = entity_type.read(request.path[1])
        if previous_entity is None:
            return Response(404)
        entity = self.marshaller_context.load(entity_type, request.input)
        if request.path == 'PATCH':
            previous_entity.patch_from(entity)
            entity = previous_entity
        entity.update()
        cache_header = entity.get_cache_header()
        response_headers = cache_header.get_cache_control_headers()
        content = self.marshaller_context.dump(entity)
        return Response(200, response_headers, content)
