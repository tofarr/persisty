from dataclasses import Field, MISSING
from typing import Optional

from pyrsistent._checked_types import get_type
from schemey import SchemaContext, get_default_schema_context

from servey.cache_control.cache_control_abc import CacheControlABC
from servey.cache_control.secure_hash_cache_control import SecureHashCacheControl

from aaaa.attr.attr import Attr
from aaaa.attr.attr_filter_op import TYPE_FILTER_OPS
from aaaa.attr.generator.default_value_generator import DefaultValueGenerator
from aaaa.attr.generator.defaults import get_default_generator_for_create, get_default_generator_for_update
from aaaa.index import Index
from aaaa.key_config.attr_key_config import ATTR_KEY_CONFIG
from aaaa.key_config.key_config_abc import KeyConfigABC
from aaaa.link.link_abc import LinkABC
from aaaa.store_meta import StoreMeta
from aaaa.util import to_snake_case
from aaaa.util.undefined import UNDEFINED


def stored(
    cls=None,
    *,
    key_config: KeyConfigABC = ATTR_KEY_CONFIG,
    cache_control: CacheControlABC = SecureHashCacheControl(),
    batch_size: int = 100,
    schema_context: Optional[SchemaContext] = None,
):
    """Decorator inspired by dataclasses, containing stored meta."""
    if schema_context is None:
        schema_context = get_default_schema_context()

    def wrapper(cls_):
        cls_dict = cls_.__dict__
        annotations = cls_dict.__annotations__
        attrs = []
        links = []
        for name, type_ in annotations.items():
            if name.startswith("__"):
                continue
            value = cls_dict.get(name, UNDEFINED)
            if isinstance(value, LinkABC):
                links.append(value)
                continue
            if isinstance(value, Attr):
                attrs.append(value)
                continue

            creatable = True
            updatable = True
            create_transform = None
            update_transform = None
            if isinstance(value, Field):
                if value.metadata.get('persisty'):
                    attrs.append(value.metadata.get('persisty'))
                    continue
                if value.default is not MISSING:
                    create_transform = DefaultValueGenerator(value.default)
                schema = value.metadata.get('schemey') or schema_context.schema_from_type(type_)
            else:
                schema = schema_context.schema_from_type(type_)
                if value is UNDEFINED:
                    creatable, create_transform = get_default_generator_for_create(name, type_)
                    updatable, update_transform = get_default_generator_for_update(name, type_)
                else:
                    create_transform = DefaultValueGenerator(value)
            db_type = get_type(type_)
            permitted_filter_ops = TYPE_FILTER_OPS.get(db_type) or Attr.permitted_filter_ops
            attr = Attr(
                name=name,
                type=db_type,
                schema=schema,
                creatable=creatable,
                updatable=updatable,
                create_transform=create_transform,
                update_transform=update_transform,
                permitted_filter_ops=permitted_filter_ops
            )
            attrs.append(attr)

        store_meta = StoreMeta(
            name=to_snake_case(cls_.__name__),
            attrs=tuple(attrs),
            key_config=key_config,
            cache_control=cache_control,
            batch_size=batch_size,
            description=cls_.__doc__,
            links=tuple(links),
            indexes=tuple(v for v in cls_dict.values() if isinstance(v, Index))
        )
        result = store_meta.get_stored_dataclass()
        return result

    return wrapper if cls is None else wrapper(cls)
