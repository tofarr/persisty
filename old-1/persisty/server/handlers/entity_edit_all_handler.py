from dataclasses import dataclass
from http import HTTPStatus
from typing import List

from persisty.edit import Edit
from persisty.server.handlers.entity_handler_abc import EntityHandlerABC
from persisty.server.request import Request
from persisty.server.response import Response


@dataclass(frozen=True)
class EntityEditAllHandler(EntityHandlerABC):
    max_edits: int = 100

    def match(self, request: Request) -> bool:
        matched = len(request.path) == 1 and request.method == 'POST' and self.is_param_true(request, 'edit-all')
        return matched

    def handle_request(self, request: Request) -> Response:
        entity_type = self.get_entity_type(request)
        storage = entity_type.get_storage()
        edits = self.marshaller_context.load(List[Edit[storage.item_type]], request.input)
        if len(edits) > self.max_edits:
            return Response(HTTPStatus.BAD_REQUEST)
        storage.edit_all(edits)
        return Response(HTTPStatus.OK)
