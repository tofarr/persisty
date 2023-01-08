from typing import Iterator

from servey.servey_starlette.route_factory.route_factory_abc import RouteFactoryABC
from starlette.routing import Route


class DirectoryDataStorageRouteFactory(RouteFactoryABC):

    def create_routes(self) -> Iterator[Route]:
        # if there are data storage factories, we create the endpoint.
        # otherwise nope
