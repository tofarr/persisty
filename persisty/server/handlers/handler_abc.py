from abc import abstractmethod, ABC
from datetime import datetime
from email.utils import parsedate

from persisty.cache_header import CacheHeader
from persisty.server.request import Request
from persisty.server.response import Response


class HandlerABC(ABC):
    priority: int = 100

    @abstractmethod
    def match(self, request: Request) -> bool:
        """ does this handler match the request given? """

    @abstractmethod
    def handle_request(self, request: Request) -> Response:
        """ handle the request given """

    @staticmethod
    def is_modified(request: Request, cache_header: CacheHeader) -> bool:
        if_none_match = request.headers.get('If-None-Match')
        if if_none_match is not None:
            return cache_header.cache_key != if_none_match

        if_modified_since = request.headers.get('If-Modified-Since')
        if if_modified_since and cache_header.updated_at:
            if_modified_since = datetime(*parsedate(if_modified_since)[:6])
            return if_modified_since < cache_header.updated_at

        return True

    @staticmethod
    def is_param_true(request: Request, param_name: str) -> bool:
        return request.params.get(param_name) in ['1', 'true']

    def __ne__(self, other):
        return self.priority != getattr(other, 'priority', None)

    def __lt__(self, other):
        return self.priority < getattr(other, 'priority', None)
