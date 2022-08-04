from dataclasses import dataclass
from typing import Optional, Callable

from fastapi import FastAPI, Depends
from fastapi.security import OAuth2PasswordBearer

from persisty.access_control.authorization import Authorization, ROOT
from persisty.context import PersistyContext, get_default_persisty_context
from persisty.integration.fastapi.fastapi_model_factory import FastApiModelFactory
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
        self.create_route_for_search(api)

    def create_route_for_read(self, api: FastAPI):
        item_model = self.models.item_model

        @api.get(self.get_key_path(), response_model=Optional[item_model])
        async def read(
            key: str, authorization: Authorization = Depends(self.get_authorization)
        ) -> Optional[item_model]:
            """Retrieve an item using a key"""
            storage = self.persisty_context.get_storage(
                self.storage_meta.name, authorization
            )
            item = storage.read(key)
            if item:
                parsed = item_model(**item)
                return parsed

    def create_route_for_search(self, api: FastAPI):
        search_filter_model = self.models.search_filter_model
        search_order_model = self.models.search_order_model
        result_set_model = self.models.result_set_model

        @api.get(
            f"/storage/{self.storage_meta.name}/search", response_model=result_set_model
        )
        async def search(
            search_filter: Optional[search_filter_model] = Depends(search_filter_model),
            search_order: Optional[search_order_model] = Depends(search_order_model),
            page_key: Optional[str] = None,
            limit: Optional[int] = None,
            authorization: Authorization = Depends(self.get_authorization),
        ) -> Optional[result_set_model]:
            """ Search for items """
            search_filter = search_filter.to_search_filter()
            search_order = search_order.to_search_order()
            storage = self.persisty_context.get_storage(
                self.storage_meta.name, authorization
            )
            result_set = storage.search(search_filter, search_order, page_key, limit)
            return result_set

    def get_key_path(self):
        return "/storage/" + self.storage_meta.name + "/items/{key}"
