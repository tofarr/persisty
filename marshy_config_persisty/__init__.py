from marshy.factory.impl_marshaller_factory import register_impl
from marshy.marshaller.enum_marshaller import EnumMarshaller
from marshy.marshaller_context import MarshallerContext

from marshy_config_persisty.field_filter_op_marshaller import FieldFilterOpMarshaller
from persisty.access_control.access_control import AccessControl
from persisty.access_control.access_control_abc import AccessControlABC
from persisty.access_control.field_filter_access_control import FieldFilterAccessControl
from persisty.cache_control.cache_control_abc import CacheControlABC
from persisty.cache_control.secure_hash_cache_control import SecureHashCacheControl
from persisty.cache_control.timestamp_cache_control import TimestampCacheControl
from persisty.cache_control.ttl_cache_control import TtlCacheControl
from persisty.key_config.composite_key_config import CompositeKeyConfig
from persisty.key_config.field_key_config import FieldKeyConfig
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.search_filter.and_filter import And
from persisty.search_filter.exclude_all import ExcludeAll
from persisty.search_filter.include_all import IncludeAll
from persisty.search_filter.not_filter import Not
from persisty.search_filter.or_filter import Or
from persisty.search_filter.query_filter import QueryFilter
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.field.field_filter import FieldFilter, FieldFilterOp
from persisty.storage.field.write_transform.default_value_transform import DefaultValueTransform
from persisty.storage.field.write_transform.int_sequence_generator import IntSequenceGenerator
from persisty.storage.field.write_transform.str_sequence_genrator import StrSequenceGenerator
from persisty.storage.field.write_transform.timestamp_generator import TimestampGenerator
from persisty.storage.field.write_transform.uuid_generator import UuidGenerator
from persisty.storage.field.write_transform.write_transform_abc import WriteTransformABC

priority = 100


def configure(context: MarshallerContext):
    context.register_marshaller(FieldFilterOpMarshaller())
    configure_search_filters(context)
    configure_key_configs(context)
    configure_write_transforms(context)
    configure_access_control(context)
    configure_cache_control(context)


def configure_search_filters(context: MarshallerContext):
    register_impl(SearchFilterABC, And, context)
    register_impl(SearchFilterABC, Or, context)
    register_impl(SearchFilterABC, ExcludeAll, context)
    register_impl(SearchFilterABC, FieldFilter, context)
    register_impl(SearchFilterABC, IncludeAll, context)
    register_impl(SearchFilterABC, Not, context)
    register_impl(SearchFilterABC, QueryFilter, context)


def configure_key_configs(context: MarshallerContext):
    register_impl(KeyConfigABC, CompositeKeyConfig, context)
    register_impl(KeyConfigABC, FieldKeyConfig, context)


def configure_write_transforms(context: MarshallerContext):
    register_impl(WriteTransformABC, DefaultValueTransform, context)
    register_impl(WriteTransformABC, IntSequenceGenerator, context)
    register_impl(WriteTransformABC, StrSequenceGenerator, context)
    register_impl(WriteTransformABC, TimestampGenerator, context)
    register_impl(WriteTransformABC, UuidGenerator, context)


def configure_access_control(context: MarshallerContext):
    register_impl(AccessControlABC, FieldFilterAccessControl, context)
    register_impl(WriteTransformABC, AccessControl, context)


def configure_cache_control(context: MarshallerContext):
    register_impl(CacheControlABC, SecureHashCacheControl, context)
    register_impl(CacheControlABC, TimestampCacheControl, context)
    register_impl(CacheControlABC, TtlCacheControl, context)
