import dataclasses
import inspect
from enum import Enum
from typing import Callable, Optional, Type, List, Dict, Tuple

import strawberry
import typing_inspect
from marshy.marshaller.marshaller_abc import MarshallerABC
from strawberry.dataloader import DataLoader
from strawberry.field import StrawberryField
from strawberry.types import Info

from persisty.access_control.authorization import ROOT, Authorization
from persisty.context import PersistyContext
from persisty.field.field_filter import FieldFilterOp, FieldFilter
from persisty.field.write_transform.default_value_transform import DefaultValueTransform
from persisty.field.write_transform.write_transform_mode import WriteTransformMode
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_field import SearchOrderField
from persisty.storage.storage_meta import StorageMeta
from persisty.util import UNDEFINED


@dataclasses.dataclass
class StorageSchemaFactory:
    persisty_context: PersistyContext
    storage_meta: StorageMeta
    item_type: Optional[Type] = None
    search_filter_factory_type: Optional[Type] = None
    search_order_factory_type: Optional[Type] = None
    result_set_type: Optional[Type] = None
    create_input_type: Optional[Type] = None
    update_input_type: Optional[Type] = None

    def add_to_schema(
            self,
            query: Dict[str, StrawberryField],
            mutation: Dict[str, StrawberryField],
    ):
        _add_field(self.create_read_field(), query)
        _add_field(self.create_read_batch_field(), query)
        _add_field(self.create_search_field(), query)
        _add_field(self.create_count_field(), query)
        _add_field(self.create_create_field(), mutation)
        _add_field(self.create_update_field(), mutation)
        _add_field(self.create_delete_field(), mutation)

    def get_authorization(self, info: Info):
        try:
            print("THIS IS VERY WRONG!!! FIX ME PLEASE!!")
            return info.context["authorization"]
        except KeyError:
            return ROOT

    def create_search_field(self) -> StrawberryField:
        search_filter_factory_type = self.get_search_filter_factory_type()
        strawberry_result_set_type = self.get_result_set_type()
        search_order_factory_type = self.get_search_order_factory_type()
        item_marshaller = self.get_marshaller_for_type(self.get_item_type())

        def resolver(
                info: Info,
                search_filter: Optional[search_filter_factory_type] = None,
                search_order: Optional[search_order_factory_type] = None,
                page_key: Optional[str] = None,
                limit: int = self.storage_meta.batch_size,
        ) -> strawberry_result_set_type:
            authorization = self.get_authorization(info)
            storage = self.get_storage(authorization)
            search_filter = self.create_search_filter(search_filter)
            search_order = self.create_search_order(search_order)
            result_set = storage.search(search_filter, search_order, page_key, limit)
            result_set.results = [
                item_marshaller.load(r) for r in result_set.results
            ]
            return result_set

        return _strawberry_field(f"search_{self.storage_meta.name}", resolver)

    def create_count_field(self) -> StrawberryField:
        search_filter_factory_type = self.get_search_filter_factory_type()

        def resolver(
                info: Info,
                search_filter: Optional[search_filter_factory_type] = None
        ) -> int:
            authorization = self.get_authorization(info)
            storage = self.get_storage(authorization)
            search_filter = self.create_search_filter(search_filter)
            count = storage.count(search_filter)
            return count

        return _strawberry_field(f"count_{self.storage_meta.name}", resolver)

    def create_read_field(self) -> StrawberryField:
        item_type = self.get_item_type()
        marshaller = self.get_marshaller_for_type(item_type)

        async def read(reads: List[Tuple[str, Authorization]]) -> List[item_type]:
            # Because the signature for this only takes one key at a time, we bundle the auth with the key
            # But all auth items should be the same, so we just use the first to get the storage
            authorization = reads[0][1]
            keys = [r[0] for r in reads]
            storage = self.get_storage(authorization)
            items = storage.read_all(keys)
            loaded = [marshaller.load(r) if r else None for r in items]
            return loaded

        loader = DataLoader(load_fn=read)

        async def resolver(key: str, info: Info) -> Optional[item_type]:
            authorization = self.get_authorization(info)
            return await loader.load((key, authorization))

        return _strawberry_field(f"read_{self.storage_meta.name}", resolver)

    def create_read_batch_field(self) -> StrawberryField:
        item_type = self.get_item_type()
        marshaller = self.get_marshaller_for_type(item_type)

        async def resolver(
                keys: List[str], info: Info
        ) -> List[Optional[item_type]]:
            authorization = self.get_authorization(info)
            storage = self.get_storage(authorization)
            read = storage.read_batch(keys)
            loaded = [marshaller.load(r) if r else None for r in read]
            return loaded

        return _strawberry_field(f"read_batch_{self.storage_meta.name}", resolver)

    def create_create_field(self) -> StrawberryField:
        item_type = self.get_item_type()
        create_input_type = self.get_create_input_type()
        input_marshaller = self.get_marshaller_for_type(create_input_type)
        item_marshaller = self.get_marshaller_for_type(item_type)

        def resolver(item: create_input_type, info: Info) -> Optional[item_type]:
            authorization = self.get_authorization(info)
            item = input_marshaller.dump(item)
            created = self.get_storage(authorization).create(item)
            if created:
                created = item_marshaller.load(created)
                return created

        return _strawberry_field(f"create_{self.storage_meta.name}", resolver)

    def create_update_field(self) -> StrawberryField:
        item_type = self.get_item_type()
        search_filter_type = self.get_search_filter_factory_type()
        update_input_type = self.get_update_input_type()
        input_marshaller = self.get_marshaller_for_type(update_input_type)
        item_marshaller = self.get_marshaller_for_type(item_type)

        def resolver(item: update_input_type, search_filter: search_filter_type, info: Info) -> Optional[item_type]:
            authorization = self.get_authorization(info)
            storage = self.persisty_context.get_storage(self.storage_meta.name, authorization)
            item = input_marshaller.dump(item)
            search_filter = self.create_search_filter(search_filter)
            updated = storage.update(item, search_filter)
            if updated:
                created = item_marshaller.load(updated)
                return created

        return _strawberry_field(f"update_{self.storage_meta.name}", resolver)

    def create_delete_field(self) -> Optional[StrawberryField]:
        if getattr(self.storage_meta.access_control, 'deletable', True) is False:
            return

        def resolver(key: str, info: Info) -> bool:
            authorization = self.get_authorization(info)
            storage = self.persisty_context.get_storage(self.storage_meta.name, authorization)
            result = storage.delete(key)
            return result

        return _strawberry_field(f"delete_{self.storage_meta.name}", resolver)

    def get_item_type(self) -> Type:
        """ Get / Create a TypeDefinition for items within the storage to be returned """
        if self.item_type:
            return self.item_type
        annotations = {}
        for field in self.storage_meta.fields:
            if not field.is_readable:
                continue
            type_ = self.wrap_type_for_strawberry(field.schema.python_type)
            annotations[field.name] = type_
        type_ = strawberry.type(
            type(
                _type_name(self.storage_meta.name),
                (),
                dict(__annotations__=annotations, __doc__=self.storage_meta.description),
            )
        )
        self.item_type = type_
        return type_

    def get_result_set_type(self):
        if self.result_set_type:
            return self.result_set_type
        name = _type_name(f"{self.storage_meta.name}_result_set")
        params = {
            "__annotations__": {"results": List[self.get_item_type()], "next_page_key": Optional[str]}
        }
        type_ = strawberry.type(type(name, (), params))
        return type_

    def get_search_filter_factory_type(self) -> Type:
        if self.search_filter_factory_type:
            return self.search_filter_factory_type
        annotations = {}
        for field in self.storage_meta.fields:
            if not field.is_readable:
                continue
            for op in field.permitted_filter_ops:
                filter_name = f"{field.name}_{op.name}"
                field_type = field.schema.python_type
                if op in (FieldFilterOp.exists, FieldFilterOp.not_exists):
                    field_type = bool
                annotations[filter_name] = Optional[field_type]
        params = {
            "__init__": _init,
            "__annotations__": annotations,
        }
        type_ = strawberry.input(type(_type_name(f"{self.storage_meta.name}_search_filter"), (), params))
        self.search_filter_factory_type = type_
        return type_

    def create_search_filter(self, obj) -> SearchFilterABC:
        search_filter = INCLUDE_ALL
        for field in self.storage_meta.fields:
            for op in field.permitted_filter_ops:
                filter_name = f"{field.name}_{op.name}"
                if hasattr(obj, filter_name):
                    value = getattr(obj, filter_name)
                    if value is not UNDEFINED:
                        # noinspection PyTypeChecker
                        value = self.get_marshaller_for_type(field.schema.python_type).load(value)
                        field_filter = FieldFilter(field.name, op, value)
                        search_filter &= field_filter
        return search_filter

    def get_search_order_factory_type(self):
        if self.search_order_factory_type:
            return self.search_order_factory_type
        fields = {f.name: f.name for f in self.storage_meta.fields if f.is_sortable}
        if not fields:
            return
        params = {
            "desc": False,
            "__init__": _init,
            "__annotations__": {
                "field": strawberry.enum(Enum(_type_name(f"{self.storage_meta.name}_search_field"), fields)),
                "desc": Optional[bool],
            },
        }
        type_ = strawberry.input(type(_type_name(f"{self.storage_meta.name}_search_order"), (), params))
        return type_

    def create_search_order(self, obj) -> Optional[SearchOrder]:
        if not obj:
            return
        field = getattr(obj, "field")
        desc = getattr(obj, "desc")
        return SearchOrder((SearchOrderField(field.value, desc),))

    def get_create_input_type(self):
        if self.create_input_type:
            return self.create_input_type
        annotations = {}
        params = {}
        for field in self.storage_meta.fields:
            if not field.is_creatable:
                continue
            type_ = self.wrap_type_for_strawberry(field.schema.python_type)
            mode = WriteTransformMode.SPECIFIED
            if field.write_transform:
                mode = field.write_transform.get_create_mode()
                if isinstance(field.write_transform,
                              DefaultValueTransform) and field.write_transform.default_value is None:
                    params[field.name] = None

            if mode == WriteTransformMode.GENERATED:
                continue
            if mode == WriteTransformMode.OPTIONAL:
                type_ = Optional[type_]

            annotations[field.name] = type_

        params['__init__'] = _init
        params["__annotations__"] = annotations
        type_ = strawberry.input(type(_type_name(f"create_{self.storage_meta.name}_input"), (), params))
        self.create_input_type = type_
        return type_

    def get_update_input_type(self):
        if self.update_input_type:
            return self.update_input_type
        annotations = {}
        for field in self.storage_meta.fields:
            if not field.is_updatable:
                continue
            type_ = self.wrap_type_for_strawberry(field.schema.python_type)
            mode = WriteTransformMode.SPECIFIED
            if field.write_transform:
                mode = field.write_transform.get_update_mode()

            if mode == WriteTransformMode.GENERATED:
                continue
            if mode == WriteTransformMode.OPTIONAL:
                type_ = Optional[type_]

            annotations[field.name] = type_

        name = _type_name(f"update_{self.storage_meta.name}_input")
        params = {
            '__init__': _init,
            '__annotations__': annotations
        }
        type_ = strawberry.input(type(name, (), params))
        self.update_input_type = type
        return type_

    def get_marshaller_for_type(self, type_: Type) -> MarshallerABC:
        marshaller = self.persisty_context.schema_context.marshaller_context.get_marshaller(type_)
        return marshaller

    def wrap_type_for_strawberry(self, type_: Type):
        """ Wrap a nested dataclass structure in strawberry types """
        origin = typing_inspect.get_origin(type_)
        if origin:
            origin = self.wrap_type_for_strawberry(type_)
            args = tuple(self.wrap_type_for_strawberry(a) for a in typing_inspect.get_args(type_))
            return origin[args]
        if dataclasses.is_dataclass(type_):
            return strawberry.type(type_, description=type_.__doc__)
        # Handle anything that strawberry has trouble with here...
        return type_

    def get_storage(self, authorization: Authorization):
        return self.persisty_context.get_storage(self.storage_meta.name, authorization)


def _strawberry_field(name: str, resolver: Callable) -> StrawberryField:
    field = strawberry.field(resolver=resolver)
    field.name = name
    field.type = inspect.signature(resolver).return_annotation
    return field


def _type_name(name: str) -> str:
    return "".join(p[:1].upper()+p[1:] for p in name.split('_'))


def _init(self, *_, **kwargs):
    for key in self.__annotations__:
        value = kwargs.get(key, UNDEFINED)
        setattr(self, key, value)


def _add_field(field: StrawberryField, fields: Dict[str, StrawberryField]):
    fields[field.name] = field
