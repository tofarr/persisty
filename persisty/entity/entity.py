from __future__ import annotations

import dataclasses
from typing import Optional, Type, TypeVar, Iterator

from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.types import ExternalItemType

from persisty.access_control.authorization import Authorization
from persisty.cache_control.cache_header import CacheHeader
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.storage.result_set import ResultSet
from persisty.storage.storage_meta import StorageMeta
from persisty.util import UNDEFINED

T = TypeVar("T")
PersistyContext_ = 'persisty.context.PersistyContext'  # Curse python's circular import nonsense :(


class Entity:
    __persisty_storage_meta__: StorageMeta
    __persisty_context__: PersistyContext_
    __persisty_dataclass_type__: Type
    __marshaller__: MarshallerABC

    def __init__(self, authorization, local_values=None, remote_values=None, **kwargs):
        self.authorization = authorization
        if not local_values:
            if remote_values:
                local_values = dataclasses.replace(remote_values)
            else:
                local_values = self.__persisty_dataclass_type__(**kwargs)
        self.__local_values__ = local_values
        self.__remote_values__ = remote_values

    def __repr__(self):
        s = ((f.name, getattr(self, f.name)) for f in self.__persisty_storage_meta__.fields)
        s = (f"'{k}': {v}" for k, v in s if v is not UNDEFINED)
        s = '{' + ", ".join(s) + '}'
        return s

    def __eq__(self, other):
        other_local_values = getattr(other, '__local_values__', UNDEFINED)
        return self.__local_values__ == other_local_values

    @classmethod
    def get_storage_meta(cls) -> StorageMeta:
        return cls.__persisty_storage_meta__

    @classmethod
    def get_marshaller(cls):
        marshaller = cls.__marshaller__
        if marshaller:
            return marshaller
        marshaller = cls.__persisty_context__.schema_context.marshaller_context.get_marshaller(
            cls.__persisty_storage_meta__.to_schema().python_type
        )
        cls.__marshaller__ = marshaller
        return marshaller

    def dump(self):
        dumped = self.get_marshaller().dump(self.__local_values__)
        return dumped

    def get_key(self) -> Optional[str]:
        key = self.get_storage_meta().key_config.to_key_str(self.dump())
        return key

    def get_cache_header(self) -> CacheHeader:
        cache_header = self.get_storage_meta().cache_control.get_cache_header(self.dump())
        return cache_header

    def get_differences(self) -> ExternalItemType:
        local_values = self.dump()
        remote_values = self.__remote_values__
        if not remote_values:
            return local_values
        remote_values = self.get_marshaller().dump(remote_values)
        return dict(set(remote_values) ^ set(local_values))

    def is_sync_required(self):
        return bool(self.get_differences())

    def read_self(self) -> T:
        key = self.get_key()
        storage_meta = self.get_storage_meta()
        context = self.__persisty_context__
        storage = context.get_storage(storage_meta.name, self.authorization)
        item = storage.read(key)
        self.__remote_values__ = self.get_marshaller().load(item)
        self.__local_values__ = dataclasses.replace(self.__remote_values__)
        return self

    @classmethod
    def read(cls, key: str, authorization: Authorization) -> T:
        storage_meta = cls.get_storage_meta()
        context = cls.__persisty_context__
        storage = context.get_storage(storage_meta.name, authorization)
        item = storage.read(key)
        local_values = cls.get_marshaller().load(item)
        entity = cls(authorization, local_values)
        return entity

    def create(self) -> T:
        storage_meta = self.get_storage_meta()
        context = self.__persisty_context__
        storage = context.get_storage(storage_meta.name, self.authorization)
        created = storage.create(self.dump())
        self.__remote_values__ = self.get_marshaller().load(created)
        self.__local_values__ = dataclasses.replace(self.__remote_values__)
        return self

    def update(self) -> T:
        storage_meta = self.get_storage_meta()
        context = self.__persisty_context__
        storage = context.get_storage(storage_meta.name, self.authorization)
        created = storage.update(self.dump())
        self.__remote_values__ = self.get_marshaller().load(created)
        self.__local_values__ = dataclasses.replace(self.__remote_values__)
        return self

    def save(self) -> T:
        key = self.get_key()
        return self.update() if key else self.create()

    def delete(self) -> bool:
        key = self.get_key()
        storage_meta = self.get_storage_meta()
        context = self.__persisty_context__
        storage = context.get_storage(storage_meta.name, self.authorization)
        result = storage.delete(key)
        return result

    @classmethod
    def search(
        cls: Type[T],
        authorization: Authorization,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
    ) -> ResultSet[T]:
        storage_meta = cls.get_storage_meta()
        context = cls.__persisty_context__
        storage = context.get_storage(storage_meta.name, authorization)
        result_set = storage.search(search_filter, search_order)
        marshaller = cls.get_marshaller()
        result_set.results = [cls(authorization, None, marshaller.load(result)) for result in result_set.results]
        return result_set

    @classmethod
    def search_all(
        cls: Type[T],
        authorization: Authorization,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
    ) -> Iterator[T]:
        storage_meta = cls.get_storage_meta()
        context = cls.__persisty_context__
        storage = context.get_storage(storage_meta.name, authorization)
        marshaller = cls.get_marshaller()
        items = storage.search_all(search_filter, search_order)
        items = (cls(authorization, None, marshaller.load(item)) for item in items)
        return items
