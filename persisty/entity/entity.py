from __future__ import annotations
from typing import Optional, Type, TypeVar

from persisty.access_control.authorization import Authorization
from persisty.cache_control.cache_header import CacheHeader
from persisty.context import get_default_persisty_context
from persisty.obj_storage.entity_archive.entity_field_descriptor import (
    EntityFieldDescriptor,
)
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.storage.result_set import ResultSet
from persisty.storage.storage_meta import StorageMeta

T = TypeVar("T")


class Entity:
    def __init__(self, authorization, **kwargs):
        self.authorization = authorization
        self.__local_values__ = kwargs
        self.__remote_values__ = None

    def __init_subclass__(cls, **kwargs):
        # cls.__storage__ = get_default_persisty_context()
        print("Init Subclass")

    def __repr__(self):
        pass

    def __eq__(self, other):
        pass

    @classmethod
    def get_storage_meta(cls) -> StorageMeta:
        return cls.__storage_meta__

    def get_key(self) -> Optional[str]:
        key = self.get_storage_meta().key_config.to_key_str(self.__local_values__)
        return key

    def get_cache_header(self) -> CacheHeader:
        cache_header = self.get_storage_meta().cache_control.get_cache_header(
            self.__local_values__
        )
        return cache_header

    def get_differences(self):
        if not self.__remote_values__:
            return {**self.__local_values__}
        return dict(set(self.__remote_values__) ^ set(self.__local_values__))

    def is_sync_required(self):
        return bool(self.get_differences())

    def read(self, key: Optional[str] = None) -> T:
        storage_meta = self.get_storage_meta()
        context = get_default_persisty_context()
        storage = context.get_storage(storage_meta.name, self.authorization)
        if key is None:
            key = storage_meta.key_config.to_key_str(self.__local_values__)
        self.__remote_values__ = storage.read(key)
        self.__local_values__ = {**self.__remote_values__}
        return self

    def create(self) -> T:
        storage_meta = self.get_storage_meta()
        context = get_default_persisty_context()
        storage = context.get_storage(storage_meta.name, self.authorization)
        self.__remote_values__ = storage.create(self.__local_values__)
        self.__local_values__ = {**self.__remote_values__}
        # Are connected entities also saved?

    def update(self) -> T:
        storage_meta = self.get_storage_meta()
        context = get_default_persisty_context()
        storage = context.get_storage(storage_meta.name, self.authorization)
        self.__remote_values__ = storage.update(self.__local_values__)
        self.__local_values__ = {**self.__remote_values__}
        # Are connected entities also saved?

    def save(self) -> T:
        key = self.get_storage_meta().key_config.to_key_str(self.__local_values__)
        return self.update() if key else self.create()

    @classmethod
    def search(
        cls: Type[T],
        authorization: Authorization,
        search_filter: SearchFilterABC,
        search_order: SearchOrder,
    ) -> ResultSet[T]:
        storage_meta = cls.get_storage_meta()
        context = get_default_persisty_context()
        storage = context.get_storage(storage_meta.name, authorization)
        storage.search()

    @classmethod
    def search_all(
        cls: Type[T],
        authorization: Authorization,
        search_filter: SearchFilterABC,
        search_order: SearchOrder,
    ) -> Iterator[T]:
        storage_meta = cls.get_storage_meta()
        context = get_default_persisty_context()
        storage = context.get_storage(storage_meta.name, authorization)
        storage.search()


class Foo(Entity):
    bar: str = EntityFieldDescriptor()


if __name__ == "__main__":
    foo = Foo()
    print(foo.bar)
    foo.bar = "foobar"
    print(foo.bar)
