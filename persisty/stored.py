import os
from dataclasses import Field, MISSING
from enum import Enum
from typing import Optional, Tuple, Type, Dict

from marshy.factory.optional_marshaller_factory import get_optional_type
from schemey import SchemaContext, get_default_schema_context

from servey.cache_control.cache_control_abc import CacheControlABC
from servey.cache_control.secure_hash_cache_control import SecureHashCacheControl

from persisty.attr.attr import Attr, DEFAULT_PERMITTED_FILTER_OPS
from persisty.attr.attr_filter_op import TYPE_FILTER_OPS, SORTABLE_TYPES
from persisty.attr.attr_type import attr_type, AttrType
from persisty.attr.generator.default_value_generator import DefaultValueGenerator
from persisty.attr.generator.defaults import (
    get_default_generator_for_create,
    get_default_generator_for_update,
)
from persisty.errors import PersistyError
from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.index.index_abc import IndexABC
from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.link.link_abc import LinkABC
from persisty.security.store_access import ALL_ACCESS, StoreAccess
from persisty.security.store_security_abc import StoreSecurityABC
from persisty.servey.action_factory_abc import ActionFactoryABC
from persisty.store_meta import StoreMeta
from persisty.util import to_snake_case
from persisty.util.undefined import UNDEFINED


# pylint: disable=R0913
def stored(
    cls=None,
    *,
    key_config: Optional[KeyConfigABC] = None,
    store_access: Optional[StoreAccess] = ALL_ACCESS,
    store_security: Optional[StoreSecurityABC] = None,
    cache_control: Optional[CacheControlABC] = None,
    batch_size: int = 100,
    schema_context: Optional[SchemaContext] = None,
    indexes: Tuple[IndexABC, ...] = tuple(),
    label_attr_names: Optional[Tuple[str, ...]] = None,
    summary_attr_names: Optional[Tuple[str, ...]] = None,
    store_factory: Optional[StoreFactoryABC] = None,
    action_factory: Optional[ActionFactoryABC] = None,
):
    """Decorator inspired by dataclasses, containing stored meta."""
    if schema_context is None:
        schema_context = get_default_schema_context()

    # pylint: disable=R0912,R0914
    def wrapper(cls_):
        nonlocal key_config, cache_control, batch_size, indexes, label_attr_names
        nonlocal summary_attr_names, store_factory, action_factory
        links_by_name = {}
        attrs_by_name = {}
        key_config, cache_control, batch_size, indexes = _derive_args(
            cls_,
            key_config,
            cache_control,
            batch_size,
            indexes,
            attrs_by_name,
            links_by_name,
        )
        cls_dict = _get_cls_dict_with_super(cls_)
        annotations = _get_cls_annotations_with_super(cls_)
        attrs_by_name = _derive_attrs(annotations, cls_dict, schema_context)

        for name, value in cls_dict.items():
            if isinstance(value, LinkABC):
                links_by_name[name] = value
                value.update_attrs(attrs_by_name)

        key_config = _derive_key_config(key_config, cls_, attrs_by_name)
        if label_attr_names:
            for label_attr_name in label_attr_names:
                if label_attr_name not in attrs_by_name:
                    raise PersistyError(
                        f"unknown_label_attr:{label_attr_name}:{cls_.__name__}"
                    )
        else:
            label_attr_name = next(
                (a.name for a in attrs_by_name.values() if a.attr_type == AttrType.STR),
                None,
            )
            if label_attr_name:
                label_attr_names = (label_attr_name,)

        # Make sure key attributes are not updatable...
        for attr_name in key_config.get_key_attrs():
            attr = attrs_by_name[attr_name]
            attr.updatable = False

        if not label_attr_names:
            label_attr_names = tuple(key_config.get_key_attrs())
        if summary_attr_names:
            for summary_attr_name in summary_attr_names:
                if summary_attr_name not in attrs_by_name:
                    raise PersistyError(
                        f"unknown_summary_attr:{summary_attr_name}:{cls_.__name__}"
                    )
        else:
            # We prioritize the label attributes
            summary_attr_names = label_attr_names

        from persisty.factory.store_factory import StoreFactory
        from persisty.servey.action_factory import ActionFactory
        from persisty.security.store_security import UNSECURED

        # noinspection PyTypeChecker
        store_meta = StoreMeta(
            name=to_snake_case(cls_.__name__),
            attrs=tuple(attrs_by_name.values()),
            key_config=key_config,
            store_access=store_access,
            store_security=store_security or UNSECURED,
            cache_control=cache_control,
            batch_size=batch_size,
            description=cls_.__doc__,
            links=tuple(links_by_name.values()),
            indexes=indexes,
            label_attr_names=label_attr_names,
            summary_attr_names=summary_attr_names,
            store_factory=store_factory or StoreFactory(),
            action_factory=action_factory or ActionFactory(),
            class_functions=_get_class_functions(cls_dict),
        )
        result = store_meta.get_stored_dataclass()
        return result

    return wrapper if cls is None else wrapper(cls)


def _is_enum(type_: Type):
    try:
        return issubclass(get_optional_type(type_), Enum)
    except TypeError:
        return False


def _derive_key_config(
    key_config: Optional[KeyConfigABC], cls: Type, attrs_by_name: Dict[str, Attr]
):
    if key_config:
        # Check that all attributes required by the key actually exist...
        for attr_name in key_config.get_key_attrs():
            if attr_name not in attrs_by_name:
                raise PersistyError(f"invalid_key_attr:{attr_name}")
        return key_config
    key_attr = attrs_by_name.get("id") or attrs_by_name.get("key")
    if not key_attr:
        raise PersistyError(f"could_not_derive_key:{cls}")
    return AttrKeyConfig(key_attr.name, key_attr.attr_type)


def _derive_filter_and_sort(type_: Type, db_type: AttrType):
    if _is_enum(type_):
        return DEFAULT_PERMITTED_FILTER_OPS, False
    return (TYPE_FILTER_OPS.get(db_type) or DEFAULT_PERMITTED_FILTER_OPS), (
        os.environ.get("PERSISTY_ATTRS_SORTABLE") != "0" and db_type in SORTABLE_TYPES
    )


# pylint: disable=R0913
def _derive_args(
    cls_: Type,
    key_config: Optional[KeyConfigABC],
    cache_control: Optional[CacheControlABC],
    batch_size: int,
    indexes: Tuple[IndexABC, ...],
    attrs_by_name: Dict[str, Attr],
    links_by_name: Dict[str, LinkABC],
):
    mro = list(cls_.__mro__)[1:-1]
    mro.reverse()
    for c in mro:
        store_meta = c.__dict__.get("__persisty_store_meta__")
        if store_meta:
            if key_config is None:
                key_config = store_meta.key_config
            if cache_control is None:
                cache_control = store_meta.cache_control
            if batch_size == 100:
                batch_size = store_meta.batch_size
            if indexes is None:
                indexes = store_meta.indexes
            links_by_name.update({link.get_name(): link for link in store_meta.links})
            attrs_by_name.update({attr.name: attr for attr in store_meta.attrs})
    if cache_control is None:
        cache_control = SecureHashCacheControl()
    return key_config, cache_control, batch_size, indexes


# pylint: disable=R0914
def _derive_attrs(
    annotations: Dict[str, Type], cls_dict: Dict, schema_context: SchemaContext
) -> Dict[str, Attr]:
    attrs_by_name = {}
    for name, type_ in annotations.items():
        if name.startswith("__"):
            continue
        value = cls_dict.get(name, UNDEFINED)
        if isinstance(value, LinkABC):
            continue
        if isinstance(value, Attr):
            attrs_by_name[value.name] = value
            continue

        creatable = True
        updatable = True
        update_generator = None
        if isinstance(value, Field):
            if value.metadata.get("persisty"):
                attrs_by_name[name] = value.metadata.get("persisty")
                continue
            if value.default is not MISSING:
                create_generator = DefaultValueGenerator(value.default)
            else:
                creatable, create_generator = get_default_generator_for_create(
                    name, type_
                )
            updatable, update_generator = get_default_generator_for_update(name, type_)
            schema = value.metadata.get("schemey") or schema_context.schema_from_type(
                type_
            )
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
        permitted_filter_ops, sortable = _derive_filter_and_sort(type, db_type)
        attr = Attr(
            name=name,
            attr_type=db_type,
            schema=schema,
            creatable=creatable,
            updatable=updatable,
            sortable=sortable,
            create_generator=create_generator,
            update_generator=update_generator,
            permitted_filter_ops=permitted_filter_ops,
        )
        attrs_by_name[name] = attr
    return attrs_by_name


def _get_cls_dict_with_super(cls: Type):
    result = {}
    for c in reversed(cls.mro()[:-1]):
        for k, v in c.__dict__.items():
            if not k.startswith("__"):
                result[k] = v
    return result


def _get_cls_annotations_with_super(cls: Type) -> Dict[str, Type]:
    result = {}
    for c in reversed(cls.mro()[:-1]):
        annotations = c.__dict__.get("__annotations__") or {}
        result.update(annotations)
    return result


def _get_class_functions(cls_dict: Dict):
    results = []
    for value in cls_dict.values():
        if callable(value):
            results.append(value)
        if isinstance(value, property):
            # I am doing this to prevent muddying the waters
            raise PersistyError("stored_classes_do_not_support_properties")
    return tuple(results)
