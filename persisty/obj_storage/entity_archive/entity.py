from typing import Optional

from persisty.cache_control.cache_header import CacheHeader
from persisty.context import get_default_persisty_context
from persisty.obj_storage.entity_archive.entity_field_descriptor import (
    EntityFieldDescriptor,
)
from persisty.storage.storage_meta import StorageMeta


class Entity:
    def __init__(self, **kwargs):
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
        return cls.__storage__.get_storage_meta()

    def get_key(self) -> Optional[str]:
        key = self.get_storage_meta().key_config.to_key_str(self.__local_values__)
        return key

    def get_cache_header(self) -> CacheHeader:
        cache_header = self.get_storage_meta().cache_control.get_cache_header(
            self.__local_values__
        )
        return cache_header

    def is_save_required(self):
        pass

    def fetch(self):
        context = get_default_persisty_context()
        storage = context.get_storage(self.get_storage_meta().name)
        pass

    def create(self):
        pass

    def update(self):
        pass

    def save(self):
        pass

    @classmethod
    def search(cls):
        pass

    @classmethod
    def search_all(cls):
        pass

    @classmethod
    def filter(cls):
        pass


class Foo(Entity):
    bar: str = EntityFieldDescriptor()


if __name__ == "__main__":
    foo = Foo()
    print(foo.bar)
    foo.bar = "foobar"
    print(foo.bar)
