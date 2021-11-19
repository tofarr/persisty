from typing import Iterator, Iterable

from attr import dataclass

from persisty.access_control.access_control_abc import AccessControlABC
from persisty.attr.attr import Attr
from persisty.attr.attr_abc import AttrABC
from persisty.cache_control.cache_control_abc import CacheControlABC
from persisty.key_config.key_config_abc import KeyConfigABC


@dataclass(frozen=True)
class EntityMeta:
    name: str
    attrs: Iterable[AttrABC]
    key_config: KeyConfigABC
    access_control: AccessControlABC
    cache_control: CacheControlABC
    filter_attrs: Iterable[Attr]
