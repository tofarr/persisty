from dataclasses import dataclass
from typing import Optional, Dict, Type, List

from marshy.types import ExternalItemType
from schemey import schema_from_type
from servey.security.authorization import Authorization

from persisty.attr.attr import Attr, DEFAULT_PERMITTED_FILTER_OPS
from persisty.attr.attr_type import AttrType
from persisty.link.link_abc import LinkABC

from typing import Generic, TypeVar

from persisty_data.data_store_abc import DataStoreABC
from persisty_data.data_store_finder_abc import find_data_stores

T = TypeVar('T')


class HasUrlCallable(Generic[T]):

    def __init__(self, key: str, data_store: DataStoreABC):
        self.key = key
        self.data_store = data_store

    def __call__(self, authorization: Optional[Authorization] = None) -> Optional[T]:
        url = self.data_store.url_for_download(authorization, self.key)
        return url


@dataclass
class HasUrl(LinkABC):
    name: Optional[str] = None
    data_store_name: Optional[str] = None
    key_attr_name: Optional[str] = None
    optional: bool = True

    def __set_name__(self, owner, name):
        self.name = name
        if self.data_store_name is None and name.endswith('_url'):
            self.data_store_name = name[:-4]
        if self.key_attr_name is None:
            self.key_attr_name = f"{self.data_store_name}_key"

    def __get__(self, obj, obj_type) -> HasUrlCallable[T]:
        return HasUrlCallable(
            key=getattr(obj, self.key_attr_name),
            data_store=self.get_linked_data_store()
        )

    async def batch_call(self, keys: List, authorization: Optional[Authorization] = None) -> List[Optional[T]]:
        if not keys:
            return []
        result = list(self.get_linked_data_store().all_urls_for_download(authorization, iter(keys)))
        return result

    def arg_extractor(self, obj):
        return [getattr(obj, self.key_attr_name)]

    def get_name(self) -> str:
        return self.name

    def get_linked_type(self, forward_ref_ns: str) -> Type[Optional[str]]:
        return Optional[str]

    def get_linked_data_store(self):
        data_store = getattr(self, '_data_store', None)
        if not data_store:
            data_store = next(
                s for s in find_data_stores() if s.get_name() == self.data_store_name
            )
            setattr(self, '_data_store', data_store)
        return data_store

    def update_attrs(self, attrs_by_name: Dict[str, Attr]):
        if self.key_attr_name in attrs_by_name:
            return
        type_ = Optional[str] if self.optional else str
        schema = schema_from_type(type_)
        attrs_by_name[self.key_attr_name] = Attr(
            self.key_attr_name,
            AttrType.STR,
            schema,
            sortable=False,
            permitted_filter_ops=DEFAULT_PERMITTED_FILTER_OPS,
        )

    def update_json_schema(self, json_schema: ExternalItemType):
        key_attr_schema = json_schema.get("properties").get(self.key_attr_name)
        key_attr_schema["persistyDataHasUrl"] = self.data_store_name
