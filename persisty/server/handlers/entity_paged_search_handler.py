from dataclasses import dataclass
from http import HTTPStatus
from typing import Optional, Type

from persisty.cache_header import CacheHeader
from persisty.obj_graph.entity_abc import EntityABC
from persisty.page import Page
from persisty.search_filter import SearchFilter, search_filter_from_dataclass
from persisty.server.handlers.entity_handler_abc import EntityHandlerABC
from persisty.server.request import Request
from persisty.server.response import Response


@dataclass(frozen=True)
class EntityPagedSearchHandler(EntityHandlerABC):
    max_limit: int = 100

    def match(self, request: Request) -> bool:
        return request.method == 'GET' and len(request.path) == 1

    def handle_request(self, request: Request) -> Response:
        entity_type = self.get_entity_type(request)
        if entity_type is None:
            return Response(HTTPStatus.NOT_FOUND)

        search_filter = self.get_search_filter(request, entity_type)
        page_key = request.params.get('page_key')
        limit = self.get_limit(request)
        selections = self.get_selections(request)
        page = entity_type.paged_search(search_filter, page_key, limit, selections)

        cache_header = CacheHeader('0').combine_with(e.get_cache_header() for e in page.items)
        response_headers = cache_header.get_cache_control_headers()
        if not self.is_modified(request, cache_header):
            return Response(HTTPStatus.NOT_MODIFIED, response_headers)

        content = self.marshaller_context.dump(page, Page[entity_type])
        return Response(HTTPStatus.OK, response_headers, content)

    def get_search_filter(self, request: Request, entity_type: Type[EntityABC]) -> Optional[SearchFilter]:
        if entity_type.__filter_class__ is not None:
            filter_obj = self.marshaller_context.load(entity_type.__filter_class__, request.params)
            search_filter = search_filter_from_dataclass(filter_obj, entity_type.get_store().item_type)
            return search_filter

    def get_limit(self, request: Request):
        limit_str = request.params.get('limit')
        if limit_str:
            limit = int(limit_str)
            if limit < 0 or limit > self.max_limit:
                raise ValueError(f'invalid_limit:{limit}')
            return limit
