from dataclasses import dataclass
from typing import Optional, Type

from servey.security.authorization import Authorization

from persisty.attr.attr_filter import AttrFilter, AttrFilterOp
from persisty.errors import PersistyError
from persisty.link.linked_store_abc import LinkedStoreABC
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store_meta import StoreMeta
from persisty.util import to_snake_case, UNDEFINED


@dataclass
class HasCountCallable:
    store_meta: StoreMeta
    search_filter: SearchFilterABC

    def __call__(self, authorization: Optional[Authorization] = None) -> int:
        store = self.store_meta.create_secured_store(authorization)
        count = store.count(search_filter=self.search_filter)
        return count


@dataclass
class HasCount(LinkedStoreABC):
    local_key_attr_name: str = "id"
    remote_key_attr_name: Optional[str] = None

    def __get__(self, obj, obj_type) -> HasCountCallable:
        key = getattr(obj, self.local_key_attr_name)
        if key is UNDEFINED:
            search_filter = EXCLUDE_ALL
        else:
            search_filter = AttrFilter(self.remote_key_attr_name, AttrFilterOp.eq, key)
        return HasCountCallable(
            store_meta=self.get_linked_store_meta(),
            search_filter=search_filter,
        )

    def __set_name__(self, owner, name):
        self.name = name
        if self.remote_key_attr_name is None:
            self.remote_key_attr_name = f"{to_snake_case(owner.__name__)}_id"
        type_ = owner.__dict__["__annotations__"].get(name)
        if type_ not in (int, None):
            raise PersistyError(f"annotation_should_be_int:{name}")

    def get_name(self) -> str:
        return self.name

    def get_linked_type(self, forward_ref_ns: str) -> Type[int]:
        return int
