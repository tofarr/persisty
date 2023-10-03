from dataclasses import dataclass
from typing import Optional, Generic, TypeVar, ForwardRef

from servey.security.authorization import Authorization

from persisty.attr.attr_filter import AttrFilter, AttrFilterOp
from persisty.link.linked_store_abc import LinkedStoreABC
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store_meta import StoreMeta
from persisty.util import to_snake_case

T = TypeVar("T")


@dataclass
class HasOneCallable(Generic[T]):
    store_meta: StoreMeta
    search_filter: SearchFilterABC
    limit: Optional[int] = None

    def __call__(self, authorization: Optional[Authorization] = None) -> Optional[T]:
        store = self.store_meta.create_secured_store(authorization)
        results = store.search_all(self.search_filter)
        result = next(results, None)
        return result


@dataclass
class HasOne(LinkedStoreABC, Generic[T]):
    name: Optional[str] = None  # Allows None so __set_name__ can exist
    local_key_attr_name: str = "id"
    remote_key_attr_name: Optional[str] = None

    def __set_name__(self, owner, name):
        self.name = name
        if self.remote_key_attr_name is None:
            self.remote_key_attr_name = f"{to_snake_case(owner.__name__)}_id"
        if self.linked_store_type is None:
            self.linked_store_type = owner.__dict__["__annotations__"][name]

    def get_name(self) -> str:
        return self.name

    def get_linked_type(self, forward_ref_ns: str) -> ForwardRef:
        return ForwardRef(
            forward_ref_ns
            + "."
            + self.get_linked_store_name().title().replace("_", "")
            + "ResultSet"
        )

    def __get__(self, obj, obj_type) -> HasOneCallable[T]:
        key = getattr(obj, self.local_key_attr_name)
        return HasOneCallable(
            store_meta=self.get_linked_store_meta(),
            search_filter=AttrFilter(self.remote_key_attr_name, AttrFilterOp.eq, key),
        )
