from __future__ import annotations

import dataclasses
from typing import Optional, Type, TypeVar, Iterator, Dict

from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.types import ExternalItemType
from schemey import Schema
from servey.security.authorization import Authorization

from servey.cache_control.cache_header import CacheHeader
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.storage.result_set import ResultSet
from persisty.storage.storage_meta import StorageMeta
from persisty.util import UNDEFINED

T = TypeVar("T")
PersistyContext_ = (
    "persisty.context.PersistyContext"  # Curse python's circular import nonsense :(
)


class Entity:
    __persisty_storage_meta__: StorageMeta
    __persisty_context__: PersistyContext_
    __persisty_dataclass_type__: Type
    __marshaller__: MarshallerABC
    __schema__: Schema

    def __init__(self, authorization, local_values=None, remote_values=None, **kwargs):
        self.__authorization__ = authorization
        if not local_values:
            if remote_values:
                local_values = dataclasses.replace(remote_values)
            else:
                local_values = self.__persisty_dataclass_type__(**kwargs)
        self.__local_values__ = local_values
        self.__remote_values__ = remote_values
        self.__trigger_descriptors__("on_init")

    def __trigger_descriptors__(self, event_type: str, kwargs: Optional[Dict] = None):
        if not kwargs:
            kwargs = dict(instance=self)
        for descriptor in self.__class__.__dict__.values():
            if hasattr(descriptor, event_type):
                callable_ = getattr(descriptor, event_type)
                callable_(**kwargs)

    def __setattr__(self, key, value):
        if key.startswith("_"):
            object.__setattr__(self, key, value)
            return
        kwargs = dict(
            instance=self, key=key, old_value=getattr(self, key), new_value=value
        )
        self.__trigger_descriptors__("before_set_attr", kwargs=kwargs)
        object.__setattr__(self, key, value)
        self.__trigger_descriptors__("after_set_attr", kwargs=kwargs)

    def __repr__(self):
        s = (
            (f.name, getattr(self, f.name))
            for f in self.__persisty_storage_meta__.fields
        )
        s = (f"'{k}': {v}" for k, v in s if v is not UNDEFINED)
        s = "{" + ", ".join(s) + "}"
        return s

    def __eq__(self, other):
        other_local_values = getattr(other, "__local_values__", UNDEFINED)
        return self.__local_values__ == other_local_values

    @classmethod
    def get_storage_meta(cls) -> StorageMeta:
        return cls.__persisty_storage_meta__

    @classmethod
    def get_marshaller(cls):
        marshaller = cls.__marshaller__
        if marshaller:
            return marshaller
        marshaller = (
            cls.__persisty_context__.schema_context.marshaller_context.get_marshaller(
                cls.__persisty_storage_meta__.to_schema().python_type
            )
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
        cache_header = self.get_storage_meta().cache_control.get_cache_header(
            self.dump()
        )
        return cache_header

    @classmethod
    def get_schema(cls):
        return cls.__schema__

    def validate(self):
        self.__schema__.validate(self.dump())

    def iter_errors(self):
        return self.__schema__.iter_errors(self.dump())

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
        storage = context.get_storage_by_name(storage_meta.name, self.__authorization__)
        item = storage.read(key)
        self.__remote_values__ = self.get_marshaller().load(item)
        self.__local_values__ = dataclasses.replace(self.__remote_values__)
        self.__trigger_descriptors__("on_init")
        return self

    @classmethod
    def read(cls, key: str, authorization: Authorization) -> T:
        storage_meta = cls.get_storage_meta()
        context = cls.__persisty_context__
        storage = context.get_storage_by_name(storage_meta.name, authorization)
        item = storage.read(key)
        local_values = cls.get_marshaller().load(item)
        entity = cls(authorization, local_values)
        return entity

    def create(self) -> T:
        self.__trigger_descriptors__("before_create")
        storage_meta = self.get_storage_meta()
        context = self.__persisty_context__
        storage = context.get_storage_by_name(storage_meta.name, self.__authorization__)
        created = storage.create(self.dump())
        self.__remote_values__ = self.get_marshaller().load(created)
        self.__local_values__ = dataclasses.replace(self.__remote_values__)
        self.__trigger_descriptors__("after_create")
        return self

    def update(self) -> T:
        self.__trigger_descriptors__("before_update")
        storage_meta = self.get_storage_meta()
        context = self.__persisty_context__
        storage = context.get_storage_by_name(storage_meta.name, self.__authorization__)
        created = storage.update(self.dump())
        self.__remote_values__ = self.get_marshaller().load(created)
        self.__local_values__ = dataclasses.replace(self.__remote_values__)
        self.__trigger_descriptors__("after_update")
        return self

    def save(self) -> T:
        key = self.get_key()
        return self.update() if key else self.create()

    def delete(self) -> bool:
        self.__trigger_descriptors__("before_delete")
        key = self.get_key()
        storage_meta = self.get_storage_meta()
        context = self.__persisty_context__
        storage = context.get_storage_by_name(storage_meta.name, self.__authorization__)
        result = storage.delete(key)
        if result:
            self.__trigger_descriptors__("after_delete")
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
        storage = context.get_storage_by_name(storage_meta.name, authorization)
        result_set = storage.search(search_filter, search_order)
        marshaller = cls.get_marshaller()
        result_set.results = [
            cls(authorization, None, marshaller.load(result))
            for result in result_set.results
        ]
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
        storage = context.get_storage_by_name(storage_meta.name, authorization)
        marshaller = cls.get_marshaller()
        items = storage.search_all(search_filter, search_order)
        items = (cls(authorization, None, marshaller.load(item)) for item in items)
        return items

    @classmethod
    def count(
        cls: Type[T],
        authorization: Authorization,
        search_filter: SearchFilterABC = INCLUDE_ALL,
    ) -> int:
        storage_meta = cls.get_storage_meta()
        context = cls.__persisty_context__
        storage = context.get_storage_by_name(storage_meta.name, authorization)
        count = storage.count(search_filter)
        return count
