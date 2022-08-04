from dataclasses import dataclass
from typing import List

from marshy.marshaller_context import MarshallerContext
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from persisty.cache_control.cache_header import CacheHeader
from persisty.rest.open_api_action import OpenApiAction
from persisty.rest.open_api_component import OpenApiComponent
from persisty.rest.open_api_method import OpenApiMethod
from persisty.rest.open_api_response import OpenApiResponse
from persisty.rest.utils import get_method, coded_response, cached_response
from persisty.storage.batch_edit import BatchEditABC
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.storage_abc import StorageABC


@dataclass
class BatchRoute:
    storage: StorageABC
    marshaller_context: MarshallerContext

    async def handle(self, request: Request) -> Response:
        method = await get_method(request)
        if method == 'GET':
            return self.read_batch(request)
        elif method == 'POST':
            return self.edit_batch(request)
        else:
            return coded_response(None)

    def read_batch(self, request: Request) -> JSONResponse:
        keys = request.query_params.getlist('key')
        items = self.storage.read_batch(keys)
        get_cache_header = self.storage.get_storage_meta().cache_control.get_cache_header
        cache_header = CacheHeader().combine_with(get_cache_header(i) for i in items if i)
        return cached_response(items, cache_header, request)

    def edit_batch(self, request: Request) -> JSONResponse:
        edits = request.json()
        edits = self.marshaller_context.load(List[BatchEditABC], edits)
        edit_results = self.storage.edit_batch(edits)
        results = self.marshaller_context.dump(edit_results, List[BatchEditResult])
        return JSONResponse(results)

    def get_actions(self) -> List[OpenApiAction]:
        actions = []
        storage_meta = self.storage.get_storage_meta()
        storage_name = storage_meta.name
        actions.append(OpenApiAction(
            path='storage/' + storage_name + '/item/{key}',
            method=OpenApiMethod.GET,
            operation_id=f'storage__{storage_name}__item__get',
            responses=(
                OpenApiResponse(200, {
                    'type': 'array',
                    'items': {'anyOf':[
                        {'const': None},
                        {'$ref': f'#components/schemas/item__{storage_name}'}
                    ]}
                }
            ),
            summary=f"Retrieve a {storage_name} using a key",
            parameters=(KEY_PARAM,)
        ))
