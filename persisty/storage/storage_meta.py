from dataclasses import dataclass, fields, field, Field
from typing import Iterator, Type

from persisty.access_control.access_control import ALL_ACCESS
from persisty.access_control.access_control_abc import AccessControlABC
from persisty.attr.attr import attr_from_field
from persisty.attr.attr_abc import AttrABC
from persisty.cache_control.cache_control_abc import CacheControlABC
from persisty.cache_control.secure_hash_cache_control import SecureHashCacheControl
from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.key_config.key_config_abc import KeyConfigABC


@dataclass(frozen=True)
class StorageMeta:
    name: str
    attrs: Iterator[AttrABC]
    key_config: KeyConfigABC = AttrKeyConfig()
    access_control: AccessControlABC = ALL_ACCESS
    cache_control: CacheControlABC = SecureHashCacheControl()

    def to_dataclass(self) -> Type:
        fields_ = tuple(self._fields())
        type_ = dataclass(type(self.name, tuple(), fields_))
        return type_

    def _fields(self) -> Iterator[Field]:
        for attr in self.attrs:
            f = field(default=None, metadata=dict(schema=attr.schema))
            f.name = attr.name
            f.type = attr.type
            yield f


def storage_meta_from_dataclass(cls: Type) -> StorageMeta:
    if hasattr(cls, '__storage_meta__'):
        return cls.__storage_meta__()
    # noinspection PyDataclass
    meta = StorageMeta(
        name=cls.__name__,
        attrs=tuple(attr_from_field(f) for f in fields(cls))
    )
    return meta
