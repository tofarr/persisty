from marshy.marshaller.union_marshaller import implementation_marshaller
from marshy.marshaller_context import MarshallerContext

from marshy_config_persisty.access_control_marshaller import AccessControlMarshaller
from marshy_config_persisty.edit_marshaller_factory import EditMarshallerFactory
from marshy_config_persisty.page_marshaller_factory import PageMarshallerFactory
from persisty.access_control.access_control import AccessControl
from persisty.access_control.access_control_abc import AccessControlABC
from persisty.cache_control.cache_control_abc import CacheControlABC
from persisty.cache_control.secure_hash_cache_control import SecureHashCacheControl
from persisty.cache_control.timestamp_cache_control import TimestampCacheControl
from persisty.cache_control.ttl_cache_control import TTLCacheControl
from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.key_config.key_config_abc import KeyConfigABC

priority = 100


def configure(context: MarshallerContext):
    # Generic types require special marshallers...
    context.register_factory(PageMarshallerFactory())
    context.register_factory(EditMarshallerFactory())
    # Polymorphic types require that we specify implementations up front
    context.register_marshaller(implementation_marshaller(AccessControlABC, (AccessControl,), context))
    context.register_marshaller(AccessControlMarshaller())
    context.register_marshaller(
        implementation_marshaller(CacheControlABC, (
            SecureHashCacheControl, TimestampCacheControl, TTLCacheControl
        ), context)
    )
    context.register_marshaller(implementation_marshaller(KeyConfigABC, (AttrKeyConfig,), context))
