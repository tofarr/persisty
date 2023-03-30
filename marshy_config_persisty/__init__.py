import logging

from marshy.factory.impl_marshaller_factory import register_impl
from marshy.marshaller.obj_marshaller import AttrConfig
from marshy.marshaller.property_marshaller import PropertyConfig
from marshy.marshaller_context import MarshallerContext
from marshy_config_servey import raise_non_ignored

from servey.cache_control.cache_control_abc import CacheControlABC
from servey.cache_control.secure_hash_cache_control import SecureHashCacheControl
from servey.cache_control.timestamp_cache_control import TimestampCacheControl
from servey.cache_control.ttl_cache_control import TtlCacheControl
from servey.servey_aws.serverless.yml_config.yml_config_abc import YmlConfigABC

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.attr.generator.default_value_generator import DefaultValueGenerator
from persisty.attr.generator.timestamp_generator import TimestampGenerator
from persisty.attr.generator.uuid_generator import UuidGenerator
from persisty.finder.module_store_finder import ModuleStoreFinder
from persisty.finder.store_finder_abc import StoreFactoryFinderABC
from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.key_config.composite_key_config import CompositeKeyConfig
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
from persisty.util import UNDEFINED

priority = 100
LOGGER = logging.getLogger(__name__)


def configure(context: MarshallerContext):
    # A bit hacky, but we want NONE in output, but NOT undefined
    AttrConfig.filter_dumped_values = (UNDEFINED,)
    PropertyConfig.filter_dumped_values = (UNDEFINED,)

    configure_search_filters(context)
    configure_key_configs(context)
    configure_attr_value_generator(context)
    configure_cache_control(context)
    configure_links(context)
    configure_finders(context)

    configure_sqlalchemy(context)
    configure_celery(context)
    configure_serverless(context)


def configure_search_filters(context: MarshallerContext):
    register_impl(SearchFilterABC, And, context)
    register_impl(SearchFilterABC, Or, context)
    register_impl(SearchFilterABC, ExcludeAll, context)
    register_impl(SearchFilterABC, AttrFilter, context)
    register_impl(SearchFilterABC, IncludeAll, context)
    register_impl(SearchFilterABC, Not, context)
    register_impl(SearchFilterABC, QueryFilter, context)


def configure_key_configs(context: MarshallerContext):
    register_impl(KeyConfigABC, CompositeKeyConfig, context)
    register_impl(KeyConfigABC, AttrKeyConfig, context)


def configure_attr_value_generator(context: MarshallerContext):
    register_impl(AttrValueGeneratorABC, DefaultValueGenerator, context)
    register_impl(AttrValueGeneratorABC, TimestampGenerator, context)
    register_impl(AttrValueGeneratorABC, UuidGenerator, context)
    register_impl(AttrValueGeneratorABC, TimestampGenerator, context)
    register_impl(AttrValueGeneratorABC, UuidGenerator, context)


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
    except ModuleNotFoundError as e:
        msg = str(e)
        if msg.startswith("No module named '"):
            module_name = msg[len("No module named '"):-1]
            if module_name == 'sqlalchemy':
                return
        raise_non_ignored(e)


def configure_finders(context: MarshallerContext):
    register_impl(StoreFactoryFinderABC, ModuleStoreFinder, context)


def configure_celery(context: MarshallerContext):
    try:
        from servey.servey_celery.celery_config.celery_config_abc import CeleryConfigABC
        from persisty.trigger.celery_store_trigger_config import (
            CeleryStoreTriggerConfig,
        )

        register_impl(CeleryConfigABC, CeleryStoreTriggerConfig, context)
    except ModuleNotFoundError as e:
        raise_non_ignored(e)


def configure_serverless(context: MarshallerContext):
    try:
        from persisty.migration.serverless.dynamodb_yml_config import DynamodbYmlConfig

        register_impl(YmlConfigABC, DynamodbYmlConfig, context)
    except ModuleNotFoundError as e:
        raise_non_ignored(e)
