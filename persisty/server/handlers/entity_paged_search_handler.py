import json
from dataclasses import dataclass
from http import HTTPStatus
from typing import Optional, Type

from marshy.marshaller_context import MarshallerContext

from persisty.cache_header import CacheHeader
from persisty.entity.entity_abc import EntityABC
from persisty.page import Page
from persisty.server.handlers.entity_handler_abc import EntityHandlerABC
from persisty.server.request import Request
from persisty.server.response import Response
from persisty.storage.storage_filter import StorageFilter, storage_filter_from_dataclass


@dataclass(frozen=True)
class EntityPagedSearchHandler(EntityHandlerABC):
    max_limit: int = 100

    def match(self, request: Request) -> bool:
        return request.method == 'GET' and len(request.path) == 1

    def handle_request(self, request: Request) -> Response:
        entity_type = self.get_entity_type(request)
        if entity_type is None:
            return Response(HTTPStatus.NOT_FOUND)

        storage_filter = self.get_storage_filter(request, entity_type)
        page_key = request.params.get('page_key')
        limit = self.get_limit(request)
        selections = self.get_selections(request)
        page = entity_type.paged_search(storage_filter, page_key, limit, selections)

        cache_header = CacheHeader('0').combine_with(e.get_cache_header() for e in page.items)
        response_headers = cache_header.get_cache_control_headers()
        if not self.is_modified(request, cache_header):
            return Response(HTTPStatus.NOT_MODIFIED, response_headers)

        content = self.marshaller_context.dump(page, Page[entity_type])
        return Response(HTTPStatus.OK, response_headers, content)

    def get_limit(self, request: Request):
        limit_str = request.params.get('limit')
        if limit_str:
            limit = int(limit_str[0])
            if limit < 0 or limit > self.max_limit:
                raise ValueError(f'invalid_limit:{limit}')
            return limit

    def get_storage_filter(self, request: Request, entity_type: Type[EntityABC]) -> Optional[StorageFilter]:
        if 'storage_filter_json' in request.params:
            json_data = json.loads(request.params['storage_filter_json'][0])
            storage_filter = self.marshaller_context.load(StorageFilter, json_data)
            return storage_filter
        if entity_type.__filter_class__ is not None:
            filter_obj = self.marshaller_context.load(entity_type.__filter_class__, request.params)
            storage_filter = storage_filter_from_dataclass(filter_obj, entity_type.get_storage().item_type)
            return storage_filter
