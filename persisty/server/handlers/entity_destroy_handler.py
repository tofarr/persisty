from http import HTTPStatus

from persisty.server.handlers.entity_handler_abc import EntityHandlerABC
from persisty.server.request import Request
from persisty.server.response import Response


class EntityDestroyHandler(EntityHandlerABC):

    def match(self, request: Request) -> bool:
        if len(request.path) != 2:
            return False
        if request.method == 'DELETE':
            return True
        return request.method == 'POST' and self.is_param_true(request, 'delete')

    def handle_request(self, request: Request) -> Response:
        entity_type = self.get_entity_type(request)
        if entity_type is None:
            return Response(HTTPStatus.NOT_FOUND)
        key = request.path[1]
        result = entity_type.get_storage().destroy(key)
        return Response(HTTPStatus.OK if result else HTTPStatus.NOT_FOUND)
