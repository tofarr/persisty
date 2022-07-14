from dataclasses import dataclass
from http import HTTPStatus

from persisty.server.handlers.handler_abc import HandlerABC
from persisty.server.request import Request
from persisty.server.response import Response


@dataclass
class ErrorHandler(HandlerABC):

    def match(self, request: Request) -> bool:
        return True

    def handle_request(self, request: Request) -> Response:
        return Response(HTTPStatus.INTERNAL_SERVER_ERROR)
