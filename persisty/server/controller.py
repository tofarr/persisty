from collections import Callable

from persisty.server.request import Request
from persisty.server.response import Response

RequestFilter = Callable[[Request], Request]
ResponseFilter = Callable[[Response], Response]
Handler = Callable[[Request], Response]


class Controller:


    def register_request_filter(self, request_filter: RequestFilter):
        pass

    def register_response_filter(self, response_filter: ResponseFilter):
        pass

    def register_handler(self, path: str, handler: Handler):
        pass

    def get_handler(self, path) -> Handler:
        pass

    def execute(self, request: Request) -> Response:
        pass


