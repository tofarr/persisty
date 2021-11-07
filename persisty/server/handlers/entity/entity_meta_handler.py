from dataclasses import dataclass
from typing import List

from persisty.edit import Edit
from persisty.server.handlers.entity.entity_handler_abc import EntityHandlerABC
from persisty.server.request import Request
from persisty.server.response import Response


@dataclass
class EntityMetaHandler(EntityHandlerABC):
    max_edits: int = 100

    def match(self, request: Request) -> bool:
        matched = len(request.path) == 1 and request.method == 'GET' and self.is_param_true(request, 'meta')
        return matched

    def handle_request(self, request: Request) -> Response:
        entity_type = self.get_entity_type(request)
        if entity_type is None:
            return Response(404)
        entity_type.get_name()
        entity_type.get_schemas()
        entity_type.get_store().schemas
        entity_type.get_store().capabilities

        store = entity_type.get_store()
        edits = self.marshaller_context.load(List[Edit[store.item_type]], request.input)
        if len(edits) > self.max_edits:
            return Response(400)
        store.edit_all(edits)
        return Response(200)
