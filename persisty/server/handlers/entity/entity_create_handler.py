from persisty.server.handlers.entity.entity_handler_abc import EntityHandlerABC
from persisty.server.request import Request
from persisty.server.response import Response


class EntityCreateHandler(EntityHandlerABC):

    def match(self, request: Request) -> bool:
        return request.method == 'POST' and len(request.path) == 1

    def handle_request(self, request: Request) -> Response:
        entity_type = self.get_entity_type(request)
        if entity_type is None:
            return Response(404)
        entity = self.marshaller_context.load(entity_type, request.input)
        entity.create()

        cache_header = entity.get_cache_header()
        response_headers = cache_header.get_cache_control_headers()
        content = self.marshaller_context.dump(entity)
        return Response(200, response_headers, content)
