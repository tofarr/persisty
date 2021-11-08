from dataclasses import dataclass
from typing import Iterable

from persisty.server.handlers.error_handler import ErrorHandler
from persisty.server.handlers.handler_abc import HandlerABC
from persisty.server.handlers.not_found_handler import NotFoundHandler
from persisty.server.request import Request
from persisty.server.response import Response


@dataclass(frozen=True)
class AppHandler(HandlerABC):
    handlers: Iterable[HandlerABC]
    error_handler: HandlerABC = ErrorHandler()
    not_found_handler: HandlerABC = NotFoundHandler()

    def match(self, request: Request) -> bool:
        matched = next((True for h in self.handlers if h.match(request)), False)
        return matched

    def handle_request(self, request: Request) -> Response:
        handler = next((h for h in self.handlers if h.match(request)), self.not_found_handler)
        try:
            response = handler.handle_request(request)
            return response
        except (Exception, ValueError):
            response = self.error_handler.handle_request(request)
            return response
