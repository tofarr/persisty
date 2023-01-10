from dataclasses import dataclass, Field, MISSING
from typing import Optional

from marshy import get_default_context
from marshy.marshaller_context import MarshallerContext

from servey.cache_control.cache_control_abc import CacheControlABC
from servey.cache_control.secure_hash_cache_control import SecureHashCacheControl

from persisty.errors import PersistyError
from persisty.link.link_abc import LinkABC
from persisty.storage.storage_meta import StorageMeta
from persisty.obj_storage.attr import Attr
from persisty.key_config.field_key_config import FIELD_KEY_CONFIG
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.field.write_transform.default_value_transform import (
    DefaultValueTransform,
)
from persisty.util import UNDEFINED, to_snake_case


def stored(
    cls=None,
    *,
    key_config: KeyConfigABC = FIELD_KEY_CONFIG,
    cache_control: CacheControlABC = SecureHashCacheControl(),
    batch_size: int = 100,
    marshaller_context: Optional[MarshallerContext] = None
):
    """Decorator inspired by dataclasses, containing stored meta."""
    if marshaller_context is None:
        marshaller_context = get_default_context()

    def wrapper(cls_):
        cls_dict = cls_.__dict__
        params = {k: v for k, v in cls_dict.items() if not k.startswith("__")}
        annotations = dict(**cls_.__annotations__)

        fields = []
        links = []
        for name, type_ in list(annotations.items()):
            if name.startswith("__"):
                continue
            attr = cls_dict.get(name, UNDEFINED)
            if isinstance(attr, LinkABC):
                links.append(attr)
                del annotations[name]
                del params[name]
                attr.update_params(params, annotations, fields)
                continue
            if not isinstance(attr, Attr):
                schema = None
                write_transform = UNDEFINED
                if isinstance(attr, Field):
                    schema = attr.metadata.get("schemey")
                    if attr.default is not MISSING:
                        write_transform = DefaultValueTransform(marshaller_context.dump(attr.default, type_))
                    if attr.default_factory is not MISSING:
                        raise PersistyError('factory_not_supported')
                    # We don't support factories here right now
                elif attr is not UNDEFINED:
                    write_transform = DefaultValueTransform(marshaller_context.dump(attr, type_))
                attr = Attr(
                    name=name,
                    type=annotations[name],
                    schema=schema,
                    write_transform=write_transform,
                )

            attr.populate(key_config)
            fields.append(attr.to_field())
            params[name] = UNDEFINED

        for name, value in params.items():
            if isinstance(value, LinkABC):
                annotations.pop(name, None)
                links.append(value)

        storage_meta = StorageMeta(
            name=to_snake_case(cls_.__name__),
            fields=tuple(fields),
            key_config=key_config,
            cache_control=cache_control,
            batch_size=batch_size,
            description=cls_.__doc__,
            links=tuple(links),
        )
        attrs = {
            **params,
            "__annotations__": annotations,
            "__persisty_storage_meta__": storage_meta,
        }
        wrapped = type(cls_.__name__, tuple(), attrs)
        wrapped = dataclass(wrapped)
        return wrapped

    return wrapper if cls is None else wrapper(cls)


def get_storage_meta(stored_) -> Optional[StorageMeta]:
    return getattr(stored_, "__persisty_storage_meta__", None)
