from dataclasses import dataclass
from typing import Optional, Generic, TypeVar, ForwardRef

import typing_inspect
from servey.security.authorization import Authorization

from persisty.attr.attr_filter import AttrFilter, AttrFilterOp
from persisty.errors import PersistyError
from persisty.link.linked_store_abc import LinkedStoreABC
from persisty.result_set import ResultSet
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.store_meta import StoreMeta
from persisty.util import to_snake_case

T = TypeVar("T")


@dataclass
class HasManyCallable(Generic[T]):
    store_meta: StoreMeta
    search_filter: SearchFilterABC
    search_order: Optional[SearchOrder] = None
    limit: Optional[int] = None

    def __call__(self, authorization: Optional[Authorization] = None) -> ResultSet[T]:
        store = self.store_meta.store_factory.create(self.store_meta)
        store = self.store_meta.store_security.get_secured(store, authorization)
        result_set = store.search(
            search_filter=self.search_filter,
            search_order=self.search_order,
            limit=self.limit,
        )
        return result_set


@dataclass
class HasMany(LinkedStoreABC, Generic[T]):
    name: Optional[str] = None  # Allows None so __set_name__ can exist
    local_key_attr_name: str = "id"
    remote_key_attr_name: Optional[str] = None
    limit: int = 10
    search_order: Optional[SearchOrder] = None

    def __set_name__(self, owner, name):
        self.name = name
        if self.remote_key_attr_name is None:
            self.remote_key_attr_name = f"{to_snake_case(owner.__name__)}_id"
        if self.linked_store_type is not None:
            return
        type_ = owner.__dict__["__annotations__"][name]
        if typing_inspect.get_origin(type_) is not ResultSet:
            raise PersistyError(f"not_a_result_set:{type_}")
        self.linked_store_type = typing_inspect.get_args(type_)[0]

    def get_name(self) -> str:
        return self.name

    def get_linked_type(self, forward_ref_ns: str) -> ForwardRef:
        return ForwardRef(
            forward_ref_ns
            + "."
            + self.get_linked_store_name().title().replace("_", "")
            + "ResultSet"
        )

    def __get__(self, obj, obj_type) -> HasManyCallable[T]:
        key = getattr(obj, self.local_key_attr_name)
        return HasManyCallable(
            store_meta=self.get_linked_store_meta(),
            search_filter=AttrFilter(self.remote_key_attr_name, AttrFilterOp.eq, key),
            search_order=self.search_order,
        )
