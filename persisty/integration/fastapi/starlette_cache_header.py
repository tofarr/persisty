from typing import Optional

from requests import Request, Response

from persisty.cache_control.cache_header import CacheHeader


def is_modified(cache_header: CacheHeader, request: Request):
    # If modified since
    modified_since = is_modified_since(cache_header, request)
    none_match = is_none_match(cache_header, request)
    if modified_since or none_match:
        return True
    if modified_since is None and none_match is None:
        return True
    return False


def is_modified_since(cache_header: CacheHeader, request: Request) -> Optional[bool]:
    """Determine if a resource was modified since the timestamp given. None implies unknown."""
    if_modified_since = request.headers.get("If-Modified-Since")
    if if_modified_since is None:
        return None
    if cache_header.updated_at is None:
        return None
    return cache_header.updated_at > if_modified_since


def is_none_match(cache_header: CacheHeader, request: Request) -> Optional[bool]:
    """Determine if a resource ETag matches the one in the request. None implies unknown."""
    if_none_match = request.headers.get("If-None-Match")
    if if_none_match is None:
        return None
    if cache_header.etag is None:
        return None
    return cache_header.etag != if_none_match
