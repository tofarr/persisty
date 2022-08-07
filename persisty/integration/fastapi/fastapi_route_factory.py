from dataclasses import dataclass
from typing import Optional, Callable, List

from fastapi import FastAPI, Depends, Query, Path, Body, HTTPException
from starlette import status
from starlette.requests import Request
from starlette.responses import JSONResponse

from persisty.access_control.authorization import Authorization
from persisty.cache_control.cache_header import CacheHeader
from persisty.context import PersistyContext, get_default_persisty_context
from persisty.integration.fastapi.fastapi_model_factory import FastApiModelFactory
from persisty.integration.fastapi.starlette_cache_header import is_modified
from persisty.storage.batch_edit import BatchEdit
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta


def admin_create_all_routes(
    api: FastAPI,
    get_authorization: Callable,
    persisty_context: Optional[PersistyContext] = None,
):
    if persisty_context is None:
        persisty_context = get_default_persisty_context()
    for storage_meta in persisty_context.admin_get_all_storage_meta():
        route_factory = FastApiRouteFactory(
            storage_meta, persisty_context, get_authorization
        )
        route_factory.create_routes(api)


@dataclass
class FastApiRouteFactory:
    storage_meta: StorageMeta
    persisty_context: PersistyContext
    get_authorization: Callable
    models: Optional[FastApiModelFactory] = None

    def __post_init__(self):
        if self.models is None:
            self.models = FastApiModelFactory(self.storage_meta, self.persisty_context)

    def create_routes(self, api: FastAPI):
        self.create_route_for_read(api)
        self.create_route_for_read_batch(api)
        self.create_route_for_search(api)
        self.create_route_for_count(api)
        self.create_route_for_create(api)
        self.create_route_for_update(api)
        self.create_route_for_delete(api)
        self.create_route_for_conditional_update(api)

    def get_storage(self, authorization: Authorization) -> StorageABC:
        return self.persisty_context.get_storage(self.storage_meta.name, authorization)

    def create_route_for_read(self, api: FastAPI):
        item_model = self.models.item_model
        cache_control = self.storage_meta.cache_control

        @api.get(self.get_key_path(), response_model=Optional[item_model])
        async def read(
            request: Request,
            key: str,
            authorization: Authorization = Depends(self.get_authorization),
        ) -> Optional[item_model]:
            """Retrieve an item using a key"""
            storage = self.get_storage(authorization)
            item = storage.read(key)
            if not item:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
            cache_header = cache_control.get_cache_header(item)
            if not is_modified(cache_header, request):
                return JSONResponse(None, status_code=status.HTTP_304_NOT_MODIFIED)
            return JSONResponse(item, headers=cache_header.get_http_headers())

    def create_route_for_read_batch(self, api: FastAPI):
        item_model = self.models.item_model
        cache_control = self.storage_meta.cache_control

        @api.get(
            f"/storage/{self.storage_meta.name}/batch",
            response_model=List[Optional[item_model]],
        )
        async def read_batch(
            request: Request,
            key: List[str] = Query(),
            authorization: Authorization = Depends(self.get_authorization),
        ) -> List[Optional[item_model]]:
            """Retrieve a set of items using a key"""
            storage = self.get_storage(authorization)
            items = storage.read_batch(key)
            cache_header = CacheHeader().combine_with(
                cache_control.get_cache_header(i) for i in items if i
            )
            if not is_modified(cache_header, request):
                # noinspection PyTypeChecker
                return JSONResponse(None, status_code=status.HTTP_304_NOT_MODIFIED)
            # noinspection PyTypeChecker
            return JSONResponse(items, headers=cache_header.get_http_headers())

    def create_route_for_search(self, api: FastAPI):
        search_filter_model = self.models.search_filter_model
        search_order_model = self.models.search_order_model
        result_set_model = self.models.result_set_model
        cache_control = self.storage_meta.cache_control

        @api.get(
            f"/storage/{self.storage_meta.name}/search", response_model=result_set_model
        )
        async def search(
            request: Request,
            search_filter: Optional[search_filter_model] = Depends(search_filter_model),
            search_order: Optional[search_order_model] = Depends(search_order_model),
            page_key: Optional[str] = None,
            limit: Optional[int] = None,
            authorization: Authorization = Depends(self.get_authorization),
        ) -> result_set_model:
            """Search for items"""
            search_filter = search_filter.to_search_filter()
            search_order = search_order.to_search_order()
            storage = self.get_storage(authorization)
            result_set = storage.search(search_filter, search_order, page_key, limit)
            cache_header = CacheHeader(result_set.next_page_key).combine_with(
                cache_control.get_cache_header(r) for r in result_set.results
            )
            if not is_modified(cache_header, request):
                return JSONResponse(None, status_code=status.HTTP_304_NOT_MODIFIED)
            return JSONResponse(
                dict(
                    results=result_set.results, next_page_key=result_set.next_page_key
                ),
                headers=cache_header.get_http_headers(),
            )

    def create_route_for_count(self, api: FastAPI):
        search_filter_model = self.models.search_filter_model

        @api.get(f"/storage/{self.storage_meta.name}/search", response_model=int)
        async def count(
            search_filter: Optional[search_filter_model] = Depends(search_filter_model),
            authorization: Authorization = Depends(self.get_authorization),
        ) -> int:
            """Count items"""
            search_filter = search_filter.to_search_filter()
            storage = self.get_storage(authorization)
            count_ = storage.count(search_filter)
            return count_

    def create_route_for_create(self, api: FastAPI):
        item_model = self.models.item_model
        create_input_model = self.models.create_input_model

        @api.post(
            f"/storage/{self.storage_meta.name}/item",
            response_model=Optional[item_model],
            status_code=status.HTTP_201_CREATED,
        )
        async def create(
            item: create_input_model,
            authorization: Authorization = Depends(self.get_authorization),
        ) -> Optional[item_model]:
            """Create an item"""
            storage = self.get_storage(authorization)
            item = storage.create(item.dict())
            if item:
                parsed = item_model(**item)
                return parsed

    def create_route_for_update(self, api: FastAPI):
        item_model = self.models.item_model
        update_input_model = self.models.update_input_model

        @api.patch(self.get_key_path(), response_model=Optional[item_model])
        async def update(
            key: str = Path(),
            item: update_input_model = Body(),
            authorization: Authorization = Depends(self.get_authorization),
        ) -> Optional[item_model]:
            """Update an item"""
            storage = self.get_storage(authorization)
            item = item.dict()
            # We override any key value with that specified in the Url.
            self.storage_meta.key_config.from_key_str(key)
            item = storage.update(item)
            if item:
                parsed = item_model(**item)
                return parsed

    def create_route_for_delete(self, api: FastAPI):
        item_model = self.models.item_model

        @api.delete(self.get_key_path(), response_model=Optional[item_model])
        async def delete(
            key: str = Query(),
            authorization: Authorization = Depends(self.get_authorization),
        ) -> bool:
            """Delete an items"""
            storage = self.get_storage(authorization)
            result = storage.delete(key)
            return result

    def create_route_for_conditional_update(self, api: FastAPI):
        item_model = self.models.item_model
        update_input_model = self.models.update_input_model
        search_filter_model = self.models.search_filter_model

        @api.patch(self.get_key_path(), response_model=Optional[item_model])
        async def conditional_update(
            key: str = Path(),
            item: update_input_model = Body(),
            search_filter: Optional[search_filter_model] = Body(),
            authorization: Authorization = Depends(self.get_authorization),
        ) -> Optional[item_model]:
            """Update an item"""
            search_filter = search_filter.to_search_filter()
            storage = self.get_storage(authorization)
            item = item.dict()
            # We override any key value with that specified in the Url.
            self.storage_meta.key_config.from_key_str(key)
            item = storage.update(item, search_filter)
            if item:
                parsed = item_model(**item)
                return parsed

    def create_route_for_edit_batch(self, api: FastAPI):
        item_model = self.models.item_model
        edit_model = self.models.batch_edit_model

        @api.post(
            f"/storage/{self.storage_meta.name}/batch",
            response_model=List[Optional[item_model]],
        )
        async def edit_batch(
            edits: List[edit_model] = Body(),
            authorization: Authorization = Depends(self.get_authorization),
        ) -> int:
            """Edit a batch of items and return the count of successful edits."""
            storage = self.get_storage(authorization)
            edits = [
                BatchEdit(
                    create_item=e.create_item.dict() if e.create_item else None,
                    update_item=e.update_item.dict() if e.update_item else None,
                    delete_key=e.delete_key,
                )
                for e in edits
            ]
            edit_results = storage.edit_batch(edits)
            num_success = sum(1 for r in edit_results if r.success)
            return num_success

    def get_key_path(self):
        return "/storage/" + self.storage_meta.name + "/item/{key}"
