from dataclasses import dataclass

from persisty.access_control.access_control import ALL_ACCESS
from persisty.access_control.access_control_abc import AccessControlABC
from persisty.cache_control.cache_control_abc import CacheControlABC
from persisty.cache_control.secure_hash_cache_control import SecureHashCacheControl
from persisty.storage.storage_meta import StorageMeta
from persisty.stored.attr import Attr
from persisty.key_config.field_key_config import ATTR_KEY_CONFIG
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.storage.field.write_transform.default_value_transform import DefaultValueTransform
from persisty.util import UNDEFINED


def stored(cls,
           key_config: KeyConfigABC = ATTR_KEY_CONFIG,
           access_control: AccessControlABC = ALL_ACCESS,
           cache_control: CacheControlABC = SecureHashCacheControl(),
           batch_size: int = 100):
    """ Decorator inspired by dataclasses, containing stored meta. """

    def wrapper(cls_):
        cls_dict = cls_.__dict__
        params = {k: v for k, v in cls_dict.items() if not k.startswith('__')}
        annotations = {**cls_dict['__annotations__']}
        fields = []
        for name, type_ in annotations:
            attr = cls.get(name, UNDEFINED)
            if not isinstance(attr, Attr):
                attr = Attr(
                    name=name,
                    type=annotations[name],
                    write_transform=UNDEFINED if attr is UNDEFINED else DefaultValueTransform(attr)
                )
            fields.append(attr.to_field())
            params[name] = UNDEFINED

        storage_meta = StorageMeta(
            name=cls_.__name__,
            fields=tuple(fields),
            key_config=key_config,
            access_control=access_control,
            cache_control=cache_control,
            batch_size=batch_size
        )
        attrs = {**fields, '__annotations__': annotations, '__persisty_storage_meta__': storage_meta}
        wrapped = type(cls.__name__, tuple(), attrs)
        wrapped = dataclass(wrapped)
        return wrapped

    return wrapper if cls is None else wrapper(cls)


def get_storage_meta(stored) -> StorageMeta:
    return stored.__persisty_storage_meta__
