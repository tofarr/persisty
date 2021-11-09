import json
from dataclasses import dataclass
from os import environ
from typing import Dict
from urllib.parse import parse_qs
from wsgiref.simple_server import make_server

from persisty.persisty_context import get_default_persisty_context
from persisty.server.handlers.handler_abc import HandlerABC
from persisty.server.request import Request

WSGI_HOST = 'WSGI_HOST'
WSGI_PORT = 'WSGI_PORT'


@dataclass(frozen=True)
class Wsgi:

    handler: HandlerABC

    def handle_request(self, environ, start_response):
        request = self.environ_to_request(environ)
        response = self.handler.handle_request(request)
        status = f'{response.code.value} {response.code.name}'
        content = json.dumps(response.content).encode('utf-8') if response.content is not None else None
        if content:
            response_headers = {**response.headers,
                                'Content-Type': 'application/json; charset=utf-8',
                                'Content-Length': str(len(content))}
            result = [content]
        else:
            response_headers = {**response.headers,
                                'Content-Type': 'application/json; charset=utf-8',
                                'Content-Length': '0'}
            result = []
        start_response(status, list(response_headers.items()))
        return result

    @staticmethod
    def environ_to_request(environ: Dict):
        request_body = environ['wsgi.input'].read(int(environ.get('CONTENT_LENGTH') or '0'))
        request = Request(
            method=environ['REQUEST_METHOD'],
            path=[p for p in (environ['PATH_INFO'] or '/').split('/') if p],
            headers={k[5:]: v for k, v in environ.items() if k.startswith('HTTP_')},
            params={k: v[0] for k, v in parse_qs(environ.get('QUERY_STRING')).items()},
            input=json.load(request_body) if request_body else None
        )
        return request


def start_server(host: str = None, port: int = None):
    if host is None:
        host = environ.get(WSGI_HOST) or 'localhost'
    if port is None:
        port = environ.get(WSGI_PORT)
        port = int(port) if port else 8080
    persisty_context = get_default_persisty_context()
    handler = persisty_context.get_request_handler()
    wsgi = Wsgi(handler)
    httpd = make_server(host, port, wsgi.handle_request)
    httpd.serve_forever()
