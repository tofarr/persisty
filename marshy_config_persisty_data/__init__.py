from marshy.factory.impl_marshaller_factory import register_impl
from marshy.marshaller_context import MarshallerContext
from marshy_config_servey import raise_non_ignored
from servey.servey_starlette.route_factory.route_factory_abc import RouteFactoryABC

from marshy_config_persisty_data.bytes_marshaller import BytesMarshaller
from persisty_data.data_store_route_factory import DataStoreRouteFactory

priority = 100


def configure(context: MarshallerContext):
    context.register_marshaller(BytesMarshaller(), bytes)
    try:
        register_impl(RouteFactoryABC, DataStoreRouteFactory, context)
    except ModuleNotFoundError as e:
        raise_non_ignored(e)
