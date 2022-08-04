from dataclasses import dataclass
from typing import List

from marshy.marshaller_context import MarshallerContext
from schemey.schema import str_schema
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from persisty.rest.open_api_action import OpenApiAction
from persisty.rest.open_api_component import OpenApiComponent
from persisty.rest.open_api_method import OpenApiMethod
from persisty.rest.open_api_parameter import KEY_PARAM
from persisty.rest.open_api_response import OpenApiResponse, NOT_FOUND
from persisty.rest.utils import (
    next_path_element,
    cached_response,
    get_method,
    coded_response,
)
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.storage_abc import StorageABC


@dataclass
class ItemRoute:
    storage: StorageABC
    marshaller_context: MarshallerContext

    async def handle(self, request: Request) -> Response:
        method = await get_method(request)
        if method == "GET":
            return self.read(request)
        elif method == "POST":
            return await self.post(request)
        elif method == "PATCH":
            return await self.patch(request)
        elif method == "DELETE":
            return self.delete(request)
        else:
            return coded_response(None)

    def read(self, request: Request) -> JSONResponse:
        key = next_path_element(request)
        item = self.storage.read(key)
        if not item:
            return coded_response(None)
        else:
            storage_meta = self.storage.get_storage_meta()
            cache_header = storage_meta.cache_control.get_cache_header(item)
            return cached_response(item, cache_header, request)

    def delete(self, request: Request) -> Response:
        key = next_path_element(request)
        result = self.storage.delete(key)
        return coded_response(result)

    async def post(self, request: Request) -> Response:
        key = next_path_element(request)
        item = await request.json()
        if key:
            # With a POST to an existing resource, we are already beyond standard REST, so let's roll with it
            # as a conditional patch to an existing resource
            search_filter = item.get("filter")
            item = item["updates"]
            if search_filter:
                self.marshaller_context.load(SearchFilterABC, search_filter)
            self.storage.get_storage_meta().key_config.from_key_str(key, item)
            item = self.storage.update(item, search_filter)
        else:
            item = self.storage.create(item)
        return coded_response(item)

    async def patch(self, request: Request) -> Response:
        key = next_path_element(request)
        if not key:
            return coded_response(None)
        item = await request.json()
        self.storage.get_storage_meta().key_config.from_key_str(key, item)
        item = self.storage.update(item)
        return coded_response(item)

    def get_components(self) -> List[OpenApiComponent]:
        storage_meta = self.storage.get_storage_meta()
        storage_name = storage_meta.name
        components = [
            OpenApiComponent(
                f"item__{storage_name}",
                storage_meta.to_json_schema(f"components/schemas/item__{storage_name}"),
            ),
            OpenApiComponent(
                f"update__{storage_name}",
                storage_meta.to_json_schema(
                    f"components/schemas/update__{storage_name}"
                ),
            ),
            OpenApiComponent(
                f"create__{storage_name}",
                storage_meta.to_json_schema(
                    f"components/schemas/create__{storage_name}"
                ),
            ),
        ]
        return components

    def get_actions(self) -> List[OpenApiAction]:
        actions = []
        storage_meta = self.storage.get_storage_meta()
        storage_name = storage_meta.name
        actions.append(
            OpenApiAction(
                path="storage/" + storage_name + "/item/{key}",
                method=OpenApiMethod.GET,
                operation_id=f"storage__{storage_name}__item__get",
                responses=(
                    OpenApiResponse(
                        200, {"$ref": f"#components/schemas/item__{storage_name}"}
                    ),
                    NOT_FOUND,
                ),
                summary=f"Retrieve a {storage_name} using a key",
                parameters=(KEY_PARAM,),
            )
        )
        actions.append(
            OpenApiAction(
                path="storage/" + storage_name + "/item",
                method=OpenApiMethod.POST,
                operation_id=f"storage__{storage_name}__item__create",
                responses=(
                    OpenApiResponse(
                        200, {"$ref": f"#components/schemas/item__{storage_name}"}
                    ),
                    NOT_FOUND,
                ),
                summary=f"Create {storage_name}",
                request_schema={"$ref": f"#components/schemas/update__{storage_name}"},
            )
        )
        actions.append(
            OpenApiAction(
                path="storage/" + storage_name + "/item/{key}",
                method=OpenApiMethod.POST,
                operation_id=f"storage__{storage_name}__item__conditional_update",
                responses=(
                    OpenApiResponse(
                        200, {"$ref": f"#components/schemas/item__{storage_name}"}
                    ),
                    NOT_FOUND,
                ),
                summary=f"Conditional Update {storage_name}",
                parameters=(KEY_PARAM,),
                request_schema={"$ref": f"#components/schemas/update__{storage_name}"},
            )
        )
        actions.append(
            OpenApiAction(
                path="storage/" + storage_name + "/item/{key}",
                method=OpenApiMethod.PATCH,
                operation_id=f"storage__{storage_name}__item__patch",
                responses=(
                    OpenApiResponse(
                        200, {"$ref": f"#components/schemas/item__{storage_name}"}
                    ),
                    NOT_FOUND,
                ),
                summary=f"Patch a {storage_name} using a key",
                parameters=(KEY_PARAM,),
                request_schema={"$ref": f"#components/schemas/update__{storage_name}"},
            )
        )
        actions.append(
            OpenApiAction(
                path="storage/" + storage_name + "/item/{key}",
                method=OpenApiMethod.DELETE,
                operation_id=f"storage__{storage_name}__item__patch",
                responses=(OpenApiResponse(200, str_schema().schema), NOT_FOUND),
                summary=f"Delete a {storage_name} using a key",
                parameters=(KEY_PARAM,),
            )
        )
        return actions
