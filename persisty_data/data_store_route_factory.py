from typing import Iterator

from servey.servey_starlette.route_factory.route_factory_abc import RouteFactoryABC
from starlette.routing import Route

from persisty_data.data_store_finder_abc import find_data_stores


class DataStoreRouteFactory(RouteFactoryABC):
    """
    Route factory for uploads / downloads in a hosted environment. In a non hosted environment, it is assumed
    that a service like S3 will be used, and uploads / downloads will not go through the hosted server
    """

    def create_routes(self) -> Iterator[Route]:
        for data_store_factory in find_data_stores():
            yield from data_store_factory.create_routes()
