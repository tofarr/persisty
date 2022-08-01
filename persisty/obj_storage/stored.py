from dataclasses import dataclass, Field
from typing import get_type_hints

from persisty.access_control.constants import ALL_ACCESS
from persisty.access_control.access_control_abc import AccessControlABC
from persisty.cache_control.cache_control_abc import CacheControlABC
from persisty.cache_control.secure_hash_cache_control import SecureHashCacheControl
from persisty.relation.relation_abc import RelationABC
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
    access_control: AccessControlABC = ALL_ACCESS,
    cache_control: CacheControlABC = SecureHashCacheControl(),
    batch_size: int = 100
):
    """Decorator inspired by dataclasses, containing stored meta."""

    def wrapper(cls_):
        cls_dict = cls_.__dict__
        params = {k: v for k, v in cls_dict.items() if not k.startswith("__")}
        annotations = get_type_hints(cls_, localns={cls_.__name__: cls_})

        fields = []
        relations = []
        for name, type_ in annotations.items():
            if name.startswith("__"):
                continue
            attr = cls_dict.get(name, UNDEFINED)
            if isinstance(attr, RelationABC):
                relations.append(attr)
                continue
            if not isinstance(attr, Attr):
                schema = None
                if isinstance(attr, Field):
                    schema = attr.metadata.get("schemey")
                attr = Attr(
                    name=name,
                    type=annotations[name],
                    schema=schema,
                    write_transform=UNDEFINED
                    if attr is UNDEFINED
                    else DefaultValueTransform(attr),
                )

            attr.populate(key_config)
            fields.append(attr.to_field())
            params[name] = UNDEFINED

        storage_meta = StorageMeta(
            name=to_snake_case(cls_.__name__),
            fields=tuple(fields),
            key_config=key_config,
            access_control=access_control,
            cache_control=cache_control,
            batch_size=batch_size,
            description=cls_.__doc__,
            relations=tuple(relations),
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


def get_storage_meta(stored_) -> StorageMeta:
    return stored_.__persisty_storage_meta__
