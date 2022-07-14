import importlib
import json
import pkgutil
from dataclasses import dataclass
from os import environ
from typing import Dict
from urllib.parse import parse_qs
from wsgiref.simple_server import make_server

from persisty.server.handlers.handler_abc import HandlerABC
from persisty.server.request import Request

WSGI_HOST = 'WSGI_HOST'
WSGI_PORT = 'WSGI_PORT'
CONFIG_MODULE_PREFIX = 'persisty_config_'


@dataclass(frozen=True)
class Wsgi:

    handler: HandlerABC

    def handle_request(self, env, start_response):
        request = self.env_to_request(env)
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
    def env_to_request(env: Dict):
        request_body = env['wsgi.input'].read(int(env.get('CONTENT_LENGTH') or '0'))
        request = Request(
            method=env['REQUEST_METHOD'],
            path=[p for p in (env['PATH_INFO'] or '/').split('/') if p],
            headers={k[5:]: v for k, v in env.items() if k.startswith('HTTP_')},
            params={k: v[0] for k, v in parse_qs(env.get('QUERY_STRING')).items()},
            input=json.load(request_body) if request_body else None
        )
        return request


def start_server(host: str = None, port: int = None, handler: HandlerABC = None):
    if host is None:
        host = environ.get(WSGI_HOST) or 'localhost'
    if port is None:
        port = environ.get(WSGI_PORT)
        port = int(port) if port else 8080
    if handler is None:
        handler = new_default_handler()
    wsgi = Wsgi(handler)
    httpd = make_server(host, port, wsgi.handle_request)
    httpd.serve_forever()


def new_default_handler():
    module_info = (m for m in pkgutil.iter_modules() if m.name.startswith(CONFIG_MODULE_PREFIX))
    modules = [importlib.import_module(m.name) for m in module_info]
    modules.sort(key=lambda m: m.priority, reverse=True)
    handler = None
    for module in modules:
        if hasattr(module, 'configure_server_handler'):
            handler = module.configure_server_handler(handler)
    return handler
