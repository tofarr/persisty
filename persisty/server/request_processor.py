from abc import ABC
from typing import Iterator

from persisty.http.http_request_abc import HttpRequestABC
from persisty.http.http_response import HttpResponse
from persisty.obj_graph.entity_abc import EntityABC


class HttpProcessor:

    def get(self, path: Iterator[str], request: HttpRequestABC) -> HttpResponse:
        return HttpResponse(404)

    def put(self, path: Iterator[str], request: HttpRequestABC) -> HttpResponse:
        return HttpResponse(404)

    def post(self, path: Iterator[str], request: HttpRequestABC) -> HttpResponse:
        return HttpResponse(404)

    def delete(self, path: Iterator[str], request: HttpRequestABC) -> HttpResponse:
        return HttpResponse(404)

    def options(self, path: Iterator[str], request: HttpRequestABC) -> HttpResponse:
        return HttpResponse(404)