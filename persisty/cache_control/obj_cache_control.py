from dataclasses import dataclass

from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.types import ExternalItemType

from persisty.cache_control.cache_control_abc import CacheControlABC, T
from persisty.cache_control.cache_header import CacheHeader


@dataclass(frozen=True)
class ObjCacheControl(CacheControlABC[T]):
    cache_control: CacheControlABC[ExternalItemType]
    marshaller: MarshallerABC[T]

    def get_cache_header(self, item: T) -> CacheHeader:
        dumped = self.marshaller.dump(item)
        header = self.cache_control.get_cache_header(dumped)
        return header
