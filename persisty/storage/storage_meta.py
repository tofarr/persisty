from dataclasses import dataclass, fields, field, Field
from typing import Iterator, Type, Iterable, Optional

from persisty.access_control.access_control import ALL_ACCESS
from persisty.access_control.access_control_abc import AccessControlABC
from persisty.attr.attr import attr_from_field, Attr
from persisty.attr.attr_access_control import AttrAccessControl
from persisty.attr.attr_mode import AttrMode
from persisty.cache_control.cache_control_abc import CacheControlABC
from persisty.cache_control.secure_hash_cache_control import SecureHashCacheControl
from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.key_config.key_config_abc import KeyConfigABC


@dataclass(frozen=True)
class StorageMeta:
    name: str
    attrs: Iterable[Attr]
    key_config: KeyConfigABC = AttrKeyConfig()
    access_control: AccessControlABC = ALL_ACCESS
    cache_control: CacheControlABC = SecureHashCacheControl()

    def to_dataclass(self) -> Type:
        _attributes = {f.name: f for f in self._fields()}
        _attributes['__annotations__'] = {f.name: f.type for f in self._fields()}
        type_ = dataclass(type(self.name, tuple(), _attributes))
        return type_

    def _fields(self) -> Iterator[Field]:
        for attr in self.attrs:
            f = field(default=attr.schema.default_value, metadata=dict(schema=attr.schema))
            f.name = attr.name
            f.type = attr.type
            yield f


def storage_meta_from_dataclass(cls: Type, key_config: Optional[KeyConfigABC] = None) -> StorageMeta:
    if hasattr(cls, '__storage_meta__'):
        return cls.__storage_meta__()
    if key_config is None:
        key_config = AttrKeyConfig()
    # noinspection PyDataclass
    attrs = tuple(attr_from_field(f) for f in fields(cls))
    if isinstance(key_config, AttrKeyConfig):
        attr_access_control = AttrAccessControl(
            create_mode=key_config.key_generation,
            update_mode=AttrMode.REQUIRED,
            read_mode=AttrMode.REQUIRED,
            search_mode=AttrMode.REQUIRED,
        )
        attrs = tuple(Attr(a.name, a.schema, attr_access_control) if key_config.attr == a.name else a for a in attrs)
    meta = StorageMeta(
        name=cls.__name__,
        attrs=attrs,
        key_config=key_config
    )
    return meta
