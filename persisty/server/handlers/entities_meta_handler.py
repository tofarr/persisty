from dataclasses import dataclass
from http import HTTPStatus
from typing import List

from persisty.capabilities import NO_CAPABILITIES
from persisty.persisty_meta import PersistyMeta
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
                       if e.get_store().capabilities != NO_CAPABILITIES]
        content = self.marshaller_context.dump(entity_meta, List[PersistyMeta])
        return Response(HTTPStatus.OK, content=content)
