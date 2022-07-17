import json
from dataclasses import dataclass
from http import HTTPStatus
from typing import Optional, Type

from persisty.entity.entity_abc import EntityABC
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.server.handlers.entity_handler_abc import EntityHandlerABC
from persisty.server.request import Request
from persisty.server.response import Response
from persisty.storage.storage_filter import storage_filter_from_dataclass


@dataclass(frozen=True)
class EntityCountHandler(EntityHandlerABC):
    max_limit: int = 100

    def match(self, request: Request) -> bool:
        return request.method == 'GET' and len(request.path) == 1 and self.is_param_true(request, 'count')

    def handle_request(self, request: Request) -> Response:
        entity_type = self.get_entity_type(request)
        if entity_type is None:
            return Response(HTTPStatus.NOT_FOUND)

        item_filter = self.get_item_filter(request, entity_type)
        count = entity_type.count(item_filter)

        return Response(HTTPStatus.OK, content=count)

    def get_item_filter(self, request: Request, entity_type: Type[EntityABC]) -> Optional[ItemFilterABC]:
        if 'item_filter_json' in request.params:
            json_data = json.loads(request.params['item_filter_json'][0])
            item_filter = self.marshaller_context.load(ItemFilterABC, json_data)
            return item_filter
        if entity_type.__filter_class__ is not None:
            filter_obj = self.marshaller_context.load(entity_type.__filter_class__, request.params)
            storage_filter = storage_filter_from_dataclass(filter_obj, entity_type.get_storage().item_type)
            return storage_filter.search_filter
