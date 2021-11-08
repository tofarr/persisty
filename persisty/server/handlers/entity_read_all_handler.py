from dataclasses import dataclass
from http import HTTPStatus
from typing import Optional, List

from persisty.obj_graph.entity_abc import EntityABC
from persisty.server.handlers.entity_handler_abc import EntityHandlerABC
from persisty.server.request import Request
from persisty.server.response import Response


@dataclass(frozen=True)
class EntityReadAllHandler(EntityHandlerABC):
    max_keys: int = 100

    def match(self, request: Request) -> bool:
        return request.method == 'GET' and len(request.path) == 1 and self.get_keys(request) is not None

    def handle_request(self, request: Request) -> Response:
        entity_type = self.get_entity_type(request)
        keys = self.get_keys(request)
        if entity_type is None or keys is None:
            return Response(HTTPStatus.NOT_FOUND)

        if len(keys) > self.max_keys:
            return Response(HTTPStatus.BAD_REQUEST)  # Bad request

        selections = self.get_selections(request)
        entities = list(entity_type.read_all(keys=keys, error_on_missing=False, selections=selections))
        cache_header = self.get_cache_header(entities)
        if cache_header is None:
            response_headers = {}
        else:
            response_headers = cache_header.get_cache_control_headers()
            if not self.is_modified(request, cache_header):
                return Response(HTTPStatus.NOT_MODIFIED, response_headers)

        content = self.marshaller_context.dump(entities, List[entity_type])
        return Response(HTTPStatus.OK, response_headers, content)

    @staticmethod
    def get_keys(request: Request) -> Optional[List[str]]:
        keys = request.params.get('keys')
        if keys is None:
            return None
        keys = keys.split('~')
        return keys

    @staticmethod
    def get_cache_header(entities: List[EntityABC]):
        cache_headers = (e.get_cache_header() for e in entities if e is not None)
        cache_header = next(cache_headers, None)
        if cache_header is not None:
            cache_header = cache_header.combine_with(cache_headers)
        return cache_header
