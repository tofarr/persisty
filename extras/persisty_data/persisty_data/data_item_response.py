import dataclasses
from email.utils import parsedate_to_datetime
from typing import Mapping, Optional

import marshy
from servey.cache_control.cache_control_abc import CacheControlABC
from starlette.datastructures import MutableHeaders
from starlette.responses import Response
from starlette.types import Scope, Receive, Send

from persisty_data.data_item_abc import DataItemABC


class DataItemResponse(Response):

    # noinspection PyMissingConstructor
    def __init__(
        self, status_code, headers, data_item: DataItemABC, buffer_size: int = 64 * 1024
    ):
        self.status_code = status_code
        self._headers = MutableHeaders(headers=headers)
        self.data_item = data_item
        self.buffer_size = buffer_size

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": self._headers.raw,
            }
        )
        with self.data_item.get_data_reader() as reader:
            more_body = True
            while more_body:
                buffer = reader.read(self.buffer_size)
                more_body = len(buffer) == self.buffer_size
                await send(
                    {
                        "type": "http.response.body",
                        "body": buffer,
                        "more_body": more_body,
                    }
                )


def data_item_response(
    request_headers: Mapping[str, str],
    data_item: Optional[DataItemABC],
    cache_control: CacheControlABC,
) -> Response:
    if not data_item:
        return Response(status_code=404)
    cache_header = cache_control.get_cache_header(marshy.dump(data_item))
    if cache_header.etag:
        cache_header = dataclasses.replace(cache_header, etag=data_item.etag)

    http_headers = cache_header.get_http_headers()
    if data_item.content_type:
        http_headers["content-type"] = data_item.content_type

    if_none_match = request_headers.get("If-None-Match")
    if_modified_since = request_headers.get("If-Modified-Since")
    if if_none_match and cache_header.etag:
        if cache_header.etag == if_none_match:
            return Response(status_code=304, headers=http_headers)
    elif if_modified_since and cache_header.updated_at:
        if_modified_since_date = parsedate_to_datetime(if_modified_since)
        if if_modified_since_date >= cache_header.updated_at:
            return Response(status_code=304, headers=http_headers)
    http_headers["content-length"] = str(data_item.size)
    response = DataItemResponse(
        status_code=200, headers=http_headers, data_item=data_item
    )
    return response
