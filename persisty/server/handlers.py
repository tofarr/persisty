from datetime import datetime
from email.utils import formatdate, parsedate
from typing import Optional, List

from marshy.marshaller_context import MarshallerContext

from persisty import PersistyContext
from persisty.cache_header import CacheHeader
from persisty.obj_graph.selection_set import from_selection_set_list, SelectionSet
from persisty.page import Page
from persisty.search_filter import search_filter_from_dataclass
from persisty.server.request import Request
from persisty.server.response import Response


@dataclass
class Handlers:
    persisty_context: PersistyContext
    marshaller_context: MarshallerContext

    def execute(self, request: Request) -> Response:
        raise ValueError(self, 'not_implemented')
    
    def entities_paged_search(self, request: Request) -> Response:
        raise ValueError(self, 'not_implemented')

    def entities_read(self, request: Request) -> Response:
        raise ValueError(self, 'not_implemented')

    # TODO: CREATE, UPDATE, DESTROY ENTITIES?
    
    def entity_create(self, request: Request) -> Response:
        raise ValueError(self, 'not_implemented')

    @staticmethod
    def _parse_selections(request: Request) -> Optional[SelectionSet]:
        selections_str = request.params.get('selections')
        if not selections_str:
            return
        selections_list = selections_str.split('~')
        selections = from_selection_set_list(selections_list)
        return selections

    def entity_paged_search(self, request: Request) -> Response:
        entity_type = request.path[0]
        if not self.persisty_context.has_entity(entity_type):
            return Response(404)
        params = request.params
        entity_type = self.persisty_context.get_entity(entity_type)
        search_filter = None
        if entity_type.__filter_class__ is not None:
            filter_obj = self.marshaller_context.load(entity_type.__filter_class__, params)
            search_filter = search_filter_from_dataclass(filter_obj)
        page_key = params.get('page_key')
        limit = params.get('limit')
        if limit:
            limit = int(limit)
        selections = self._parse_selections(request)
        page = entity_type.paged_search(search_filter, page_key, limit, selections)

        cache_header = CacheHeader('0').combine_with(e.get_cache_header() for e in page.items)
        response_headers = cache_header.get_cache_control_headers()
        if_none_match = request.headers.get('If-None-Match')
        if cache_header.cache_key == if_none_match:
            return Response(304, response_headers)

        content = self.marshaller_context.dump(page, Page[entity_type])
        return Response(200, response_headers, content)

    def entity_update(self, request: Request) -> Response:
        raise ValueError(self, 'not_implemented')

    def entity_destroy(self, request: Request) -> Response:
        raise ValueError(self, 'not_implemented')

    def entity_paged_search(self, request: Request) -> Response:
        raise ValueError(self, 'not_implemented')
    
    def entity_edit_all(self, request: Request) -> Response:
        raise ValueError(self, 'not_implemented')
