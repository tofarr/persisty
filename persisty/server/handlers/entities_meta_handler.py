from http import HTTPStatus
from typing import List

from persisty.server.handlers.entity_handler_abc import EntityHandlerABC
from persisty.server.request import Request
from persisty.server.response import Response
from persisty.storage.storage_meta import StorageMeta


class EntitiesMetaHandler(EntityHandlerABC):

    def match(self, request: Request) -> bool:
        matched = len(request.path) == 0 and request.method == 'GET'
        return matched

    def handle_request(self, request: Request) -> Response:
        entity_meta = (e.get_meta() for e in self.entity_context.get_entities())
        accessible_meta = [m for m in entity_meta if m.access_control.is_meta_accessible]
        content = self.marshaller_context.dump(accessible_meta, List[StorageMeta])
        return Response(HTTPStatus.OK, content=content)
