import logging

from marshy.factory.impl_marshaller_factory import register_impl
from marshy.marshaller.obj_marshaller import AttrConfig
from marshy.marshaller.property_marshaller import PropertyConfig
from marshy.marshaller_context import MarshallerContext

from marshy_config_persisty.field_filter_op_marshaller import FieldFilterOpMarshaller
from persisty.access_control.access_control import AccessControl
from persisty.access_control.access_control_abc import AccessControlABC
from persisty.access_control.factory.access_control_factory_abc import (
    AccessControlFactoryABC,
)
from persisty.access_control.factory.default_access_control_factory import (
    DefaultAccessControlFactory,
)
from persisty.access_control.factory.scope_access_control_factory import (
    ScopeAccessControlFactory,
)
from persisty.access_control.field_filter_access_control import FieldFilterAccessControl
from servey.cache_control.cache_control_abc import CacheControlABC
from servey.cache_control.secure_hash_cache_control import SecureHashCacheControl
from servey.cache_control.timestamp_cache_control import TimestampCacheControl
from servey.cache_control.ttl_cache_control import TtlCacheControl
from persisty.finder.module_storage_factory_finder import ModuleStorageFactoryFinder
from persisty.finder.storage_factory_finder_abc import StorageFactoryFinderABC
from persisty.key_config.composite_key_config import CompositeKeyConfig
from persisty.key_config.field_key_config import FieldKeyConfig
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.link.belongs_to import BelongsTo
from persisty.link.has_count import HasCount
from persisty.link.has_many import HasMany
from persisty.link.link_abc import LinkABC
from persisty.search_filter.and_filter import And
from persisty.search_filter.exclude_all import ExcludeAll
from persisty.search_filter.include_all import IncludeAll
from persisty.search_filter.not_filter import Not
from persisty.search_filter.or_filter import Or
from persisty.search_filter.query_filter import QueryFilter
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.field.field_filter import FieldFilter
from persisty.field.write_transform.default_value_transform import (
    DefaultValueTransform,
)
from persisty.field.write_transform.int_sequence_generator import (
    IntSequenceGenerator,
)
from persisty.field.write_transform.str_sequence_genrator import (
    StrSequenceGenerator,
)
from persisty.field.write_transform.timestamp_generator import (
    TimestampGenerator,
)
from persisty.field.write_transform.uuid_generator import UuidGenerator
from persisty.field.write_transform.write_transform_abc import WriteTransformABC
from persisty.util import UNDEFINED

priority = 100
LOGGER = logging.getLogger(__name__)


def configure(context: MarshallerContext):
    # A bit hacky, but we want NONE in output, but NOT undefined
    AttrConfig.filter_dumped_values = (UNDEFINED,)
    PropertyConfig.filter_dumped_values = (UNDEFINED,)

    context.register_marshaller(FieldFilterOpMarshaller())
    configure_search_filters(context)
    configure_key_configs(context)
    configure_write_transforms(context)
    configure_access_control(context)
    configure_access_control_factory(context)
    configure_cache_control(context)
    configure_links(context)
    configure_finders(context)

    configure_sqlalchemy(context)
    configure_celery(context)


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
    register_impl(AccessControlABC, AccessControl, context)


def configure_access_control_factory(context: MarshallerContext):
    register_impl(AccessControlFactoryABC, DefaultAccessControlFactory, context)
    register_impl(AccessControlFactoryABC, ScopeAccessControlFactory, context)


def configure_cache_control(context: MarshallerContext):
    register_impl(CacheControlABC, SecureHashCacheControl, context)
    register_impl(CacheControlABC, TimestampCacheControl, context)
    register_impl(CacheControlABC, TtlCacheControl, context)


def configure_links(context: MarshallerContext):
    register_impl(LinkABC, BelongsTo, context)
    register_impl(LinkABC, HasMany, context)
    register_impl(LinkABC, HasCount, context)


def configure_sqlalchemy(context: MarshallerContext):
    try:
        # Local import in case sqlalchemy is not included (Optional extra)
        from marshy_config_persisty import sqlalchemy_config

        sqlalchemy_config.configure_converters(context)
        sqlalchemy_config.configure_sqlalchemy_context(context)
    except ModuleNotFoundError:
        LOGGER.info("sqlalchemy not found - skipping")


def configure_finders(context: MarshallerContext):
    register_impl(StorageFactoryFinderABC, ModuleStorageFactoryFinder, context)


def configure_celery(context: MarshallerContext):
    try:
        from servey.servey_celery.celery_config.celery_config_abc import CeleryConfigABC
        from persisty.trigger.celery_storage_trigger_config import (
            CeleryStorageTriggerConfig,
        )

        register_impl(CeleryConfigABC, CeleryStorageTriggerConfig, context)
    except ModuleNotFoundError as e:
        LOGGER.error(e)
        LOGGER.info("Celery module not found: skipping")
