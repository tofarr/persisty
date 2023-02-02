import dataclasses
from email.utils import parsedate_to_datetime
from typing import Iterator, Mapping

import marshy
from servey.security.authorization import Authorization
from starlette.datastructures import MutableHeaders
from starlette.responses import Response
from starlette.types import Scope, Receive, Send

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_attr import SearchOrderAttr
from persisty_data.chunk import Chunk
from persisty_data.content_meta import ContentMeta


class ChunkResponse(Response):

    def __init__(self, status_code, headers, chunks: Iterator[Chunk]):
        self.status_code = status_code
        self._headers = MutableHeaders(raw=headers)
        self.chunks = chunks

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": self.raw_headers,
            }
        )
        prev_chunk = None
        for chunk in self.chunks:
            if prev_chunk:
                await send({
                    "type": "http.response.body",
                    "body": prev_chunk.data,
                    "more_body": True,
                })
            prev_chunk = chunk
        await send(
            {
                "type": "http.response.body",
                "body": prev_chunk.data,
                "more_body": False,
            }
        )


def chunk_response(
    key: str,
    authorization: Authorization,
    request_headers: Mapping[str, str],
    content_meta_store_factory: StoreFactoryABC[ContentMeta],
    chunk_store_factory: StoreFactoryABC[Chunk]
) -> Response:
    content_meta_store = content_meta_store_factory.create(authorization)
    content_meta = content_meta_store.read(key)
    if not content_meta:
        return Response(status_code=404)
    cache_control = content_meta_store.get_meta().cache_control
    cache_header = cache_control.get_cache_header(marshy.dump(content_meta))
    if cache_header.etag:
        cache_header = dataclasses.replace(cache_header, etag=content_meta.etag)

    http_headers = cache_header.get_http_headers()
    http_headers['content-length'] = content_meta.size_in_bytes
    if content_meta.content_type:
        http_headers['content-type'] = content_meta.content_type

    if_match = request_headers.get("If-Match")
    if_modified_since = request_headers.get("If-Modified-Since")
    if if_match and cache_header.etag:
        if cache_header.etag == if_match:
            return Response(status_code=304, headers=http_headers)
    elif if_modified_since and cache_header.updated_at:
        if_modified_since_date = parsedate_to_datetime(if_modified_since)
        if if_modified_since_date >= cache_header.updated_at:
            return Response(status_code=304, headers=http_headers)

    chunk_store = chunk_store_factory.create(authorization)
    chunks = chunk_store.search_all(
        search_filter=AttrFilter('content_key', AttrFilterOp.eq, key),
        search_order=SearchOrder((SearchOrderAttr('part_number'),))
    )

    response = ChunkResponse(
        status_code=200,
        headers=http_headers,
        chunks=chunks
    )
    return response
