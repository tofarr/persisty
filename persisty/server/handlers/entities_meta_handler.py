from http import HTTPStatus
from typing import List

from old.persisty.capabilities import NO_CAPABILITIES
from old.persisty.persisty_meta import PersistyMeta
from persisty.server.handlers.entity_handler_abc import EntityHandlerABC
from persisty.server.request import Request
from persisty.server.response import Response


class EntitiesMetaHandler(EntityHandlerABC):

    def match(self, request: Request) -> bool:
        matched = len(request.path) == 0 and request.method == 'GET'
        return matched

    def handle_request(self, request: Request) -> Response:
        entity_meta = [e.get_meta()
                       for e in self.persisty_context.get_entities()
                       if e.get_storage().capabilities != NO_CAPABILITIES]
        content = self.marshaller_context.dump(entity_meta, List[PersistyMeta])
        return Response(HTTPStatus.OK, content=content)
