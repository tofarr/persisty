from marshy.factory.impl_marshaller_factory import register_impl
from marshy.marshaller_context import MarshallerContext
from marshy_config_servey import raise_non_ignored

from persisty_dynamic.dynamic_store_abc import DynamicStoreFactoryABC
from persisty_dynamic.impl.dynamodb_dynamic_store import DynamodbDynamicStore
from persisty_dynamic.impl.mem_dynamic_store import MemDynamicStore
from persisty_dynamic.impl.sqlalchemy_dynamic_store import SqlalchemyDynamicStore

priority = 100


def configure(context: MarshallerContext):
    try:
        register_impl(DynamicStoreFactoryABC, DynamodbDynamicStore, context)
        register_impl(DynamicStoreFactoryABC, SqlalchemyDynamicStore, context)
        register_impl(DynamicStoreFactoryABC, MemDynamicStore, context)
    except ModuleNotFoundError as e:
        raise_non_ignored(e)
