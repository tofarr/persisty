from dataclasses import Field, MISSING
from typing import Optional, Tuple

from schemey import SchemaContext, get_default_schema_context

from servey.cache_control.cache_control_abc import CacheControlABC
from servey.cache_control.secure_hash_cache_control import SecureHashCacheControl

from persisty.attr.attr import Attr, DEFAULT_PERMITTED_FILTER_OPS
from persisty.attr.attr_filter_op import TYPE_FILTER_OPS, SORTABLE_TYPES
from persisty.attr.attr_type import attr_type
from persisty.attr.generator.default_value_generator import DefaultValueGenerator
from persisty.attr.generator.defaults import (
    get_default_generator_for_create,
    get_default_generator_for_update,
)
from persisty.errors import PersistyError
from persisty.index import Index
from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.link.link_abc import LinkABC
from persisty.store_meta import StoreMeta
from persisty.util import to_snake_case
from persisty.util.undefined import UNDEFINED


def stored(
    cls=None,
    *,
    key_config: Optional[KeyConfigABC] = None,
    cache_control: CacheControlABC = SecureHashCacheControl(),
    batch_size: int = 100,
    schema_context: Optional[SchemaContext] = None,
    indexes: Tuple[Index, ...] = tuple(),
):
    """Decorator inspired by dataclasses, containing stored meta."""
    if schema_context is None:
        schema_context = get_default_schema_context()

    def wrapper(cls_):
        nonlocal key_config, cache_control, batch_size, indexes
        links = []
        mro = list(cls_.__mro__[:-1])
        for c in mro[1:]:
            store_meta = c.__dict__.get('__persisty_store_meta__')
            if store_meta:
                if key_config is None:
                    key_config = store_meta.key_config
                if isinstance(cache_control, SecureHashCacheControl):
                    cache_control = store_meta.cache_control
                if batch_size == 100:
                    batch_size = store_meta.batch_size
                if store_meta.links:
                    links.extend(store_meta.links)
                if indexes is None:
                    indexes = store_meta.indexes


        mro.reverse()
        cls_dict = {}
        annotations = {}
        for c in mro:
            cls_dict.update(**c.__dict__)
            a = c.__dict__.get("__annotations__")
            if a:
                annotations.update(**a)
        attrs = []
        for name, type_ in annotations.items():
            if name.startswith("__"):
                continue
            value = cls_dict.get(name, UNDEFINED)
            if isinstance(value, LinkABC):
                continue
            if isinstance(value, Attr):
                attrs.append(value)
                continue

            creatable = True
            updatable = True
            update_generator = None
            if isinstance(value, Field):
                if value.metadata.get("persisty"):
                    attrs.append(value.metadata.get("persisty"))
                    continue
                if value.default is not MISSING:
                    create_generator = DefaultValueGenerator(value.default)
                else:
                    creatable, create_generator = get_default_generator_for_create(
                        name, type_
                    )
                updatable, update_generator = get_default_generator_for_update(
                    name, type_
                )
                schema = value.metadata.get(
                    "schemey"
                ) or schema_context.schema_from_type(type_)
            else:
                schema = schema_context.schema_from_type(type_)
                if value is UNDEFINED:
                    creatable, create_generator = get_default_generator_for_create(
                        name, type_
                    )
                    updatable, update_generator = get_default_generator_for_update(
                        name, type_
                    )
                else:
                    create_generator = DefaultValueGenerator(value)
            db_type = attr_type(type_)
            permitted_filter_ops = (
                TYPE_FILTER_OPS.get(db_type) or DEFAULT_PERMITTED_FILTER_OPS
            )
            attr = Attr(
                name=name,
                attr_type=db_type,
                schema=schema,
                creatable=creatable,
                updatable=updatable,
                sortable=db_type in SORTABLE_TYPES,
                create_generator=create_generator,
                update_generator=update_generator,
                permitted_filter_ops=permitted_filter_ops,
            )
            attrs.append(attr)

        # Check that all attributes required by the key actually exist...
        key_config_ = key_config
        if key_config_:
            for attr_name in key_config_.get_key_attrs():
                if not next((True for a in attrs if a.name == attr_name), False):
                    raise PersistyError(f"invalid_key_attr:{attr_name}")
        else:
            key_attr = next((a for a in attrs if a.name in ("id", "key")), None)
            if not key_attr:
                raise PersistyError(f'could_not_derive_key:{cls}')
            key_config_ = AttrKeyConfig(key_attr.name, key_attr.attr_type)

        for name, value in cls_dict.items():
            if isinstance(value, LinkABC):
                links.append(value)
                value.update_attrs(attrs)

        store_meta = StoreMeta(
            name=to_snake_case(cls_.__name__),
            attrs=tuple(attrs),
            key_config=key_config_,
            cache_control=cache_control,
            batch_size=batch_size,
            description=cls_.__doc__,
            links=tuple(links),
            indexes=indexes,
        )
        result = store_meta.get_stored_dataclass()
        return result

    return wrapper if cls is None else wrapper(cls)
