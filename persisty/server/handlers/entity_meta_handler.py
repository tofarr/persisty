from http import HTTPStatus

from persisty.server.handlers.entity_handler_abc import EntityHandlerABC
from persisty.server.request import Request
from persisty.server.response import Response


class EntityMetaHandler(EntityHandlerABC):

    def match(self, request: Request) -> bool:
        matched = len(request.path) == 1 and request.method == 'GET' and self.is_param_true(request, 'meta')
        return matched

    def handle_request(self, request: Request) -> Response:
        entity_type = self.get_entity_type(request)
        if entity_type is None:
            return Response(HTTPStatus.NOT_FOUND)
        meta = entity_type.get_meta()
        content = self.marshaller_context.dump(meta)
        return Response(HTTPStatus.OK, content=content)
