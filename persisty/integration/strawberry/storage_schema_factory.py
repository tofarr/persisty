import dataclasses
import inspect
from enum import Enum
from typing import Callable, Optional, Type, List, Dict

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
from persisty.link.belongs_to import BelongsTo
from persisty.link.has_count import HasCount
from persisty.link.has_many import HasMany
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_factory import search_filter_dataclass_for
from persisty.search_order.search_order_factory import search_order_dataclass_for
from persisty.storage.result_set import result_set_dataclass_for
from persisty.storage.storage_meta import StorageMeta
from persisty.util import UNDEFINED, to_camel_case, to_snake_case


@dataclasses.dataclass
class StorageSchemaFactory:
    persisty_context: PersistyContext
    storage_meta: StorageMeta
    types: Dict[str, Type]
    inputs: Dict[str, Type]
    enums: Dict[str, Type]
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
        result_set_type = self.get_result_set_type()
        search_order_factory_type = self.get_search_order_factory_type()
        item_marshaller = self.get_marshaller_for_type(self.get_item_type())

        def resolver(
            info: Info,
            search_filter: Optional[search_filter_factory_type] = None,
            search_order: Optional[search_order_factory_type] = None,
            page_key: Optional[str] = None,
            limit: int = self.storage_meta.batch_size,
        ) -> result_set_type:
            authorization = self.get_authorization(info)
            storage = self.get_storage(authorization)
            search_filter = (
                search_filter.to_search_filter() if search_filter else INCLUDE_ALL
            )
            search_order = search_order.to_search_order() if search_order else None
            result_set = storage.search(search_filter, search_order, page_key, limit)
            result_set.results = [item_marshaller.load(r) for r in result_set.results]
            return result_set

        return _strawberry_field(f"search_{self.storage_meta.name}", resolver)

    def create_count_field(self) -> StrawberryField:
        search_filter_factory_type = self.get_search_filter_factory_type()

        def resolver(
            info: Info, search_filter: Optional[search_filter_factory_type] = None
        ) -> int:
            authorization = self.get_authorization(info)
            storage = self.get_storage(authorization)
            search_filter = (
                search_filter.to_search_filter() if search_filter else INCLUDE_ALL
            )
            count = storage.count(search_filter)
            return count

        return _strawberry_field(f"count_{self.storage_meta.name}", resolver)

    def get_data_loader(self, storage_name: str, info: Info) -> DataLoader:
        data_loaders = info.context.get("data_loaders")
        if not data_loaders:
            data_loaders = info.context["data_loaders"] = {}
        loader = data_loaders.get(self.storage_meta.name)
        if loader:
            return loader
        authorization = self.get_authorization(info)
        item_type = self.types.get(storage_name)
        marshaller = self.get_marshaller_for_type(item_type)

        async def read(keys: List[str]) -> List[item_type]:
            storage = self.persisty_context.get_storage(storage_name, authorization)
            items = list(storage.read_all(keys))
            loaded = [marshaller.load(r) if r else None for r in items]
            return loaded

        loader = DataLoader(load_fn=read)
        data_loaders[self.storage_meta.name] = loader
        return loader

    def create_read_field(self) -> StrawberryField:
        item_type = self.get_item_type()

        async def resolver(key: str, info: Info) -> Optional[item_type]:
            loader = self.get_data_loader(self.storage_meta.name, info)
            return await loader.load(key)

        return _strawberry_field(f"read_{self.storage_meta.name}", resolver)

    def create_read_batch_field(self) -> StrawberryField:
        item_type = self.get_item_type()
        marshaller = self.get_marshaller_for_type(item_type)

        async def resolver(keys: List[str], info: Info) -> List[Optional[item_type]]:
            authorization = self.get_authorization(info)
            storage = self.get_storage(authorization)
            read = storage.read_batch(keys)
            loaded = [marshaller.load(r) if r else None for r in read]
            return loaded

        return _strawberry_field(f"read_{self.storage_meta.name}_batch", resolver)

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

        def resolver(
            item: update_input_type,
            search_filter: Optional[search_filter_type],
            info: Info,
        ) -> Optional[item_type]:
            authorization = self.get_authorization(info)
            storage = self.persisty_context.get_storage(
                self.storage_meta.name, authorization
            )
            item = input_marshaller.dump(item)
            search_filter = (
                search_filter.to_search_filter() if search_filter else INCLUDE_ALL
            )
            updated = storage.update(item, search_filter)
            if updated:
                created = item_marshaller.load(updated)
                return created

        return _strawberry_field(f"update_{self.storage_meta.name}", resolver)

    def create_delete_field(self) -> Optional[StrawberryField]:
        if getattr(self.storage_meta.access_control, "deletable", True) is False:
            return

        def resolver(key: str, info: Info) -> bool:
            authorization = self.get_authorization(info)
            storage = self.persisty_context.get_storage(
                self.storage_meta.name, authorization
            )
            result = storage.delete(key)
            return result

        return _strawberry_field(f"delete_{self.storage_meta.name}", resolver)

    def get_item_type(self) -> Type:
        """Get / Create a TypeDefinition for items within the storage to be returned"""
        item_type = self.types.get(self.storage_meta.name)
        if item_type:
            return item_type
        annotations = {}
        params = {
            "__doc__": self.storage_meta.description,
            "__annotations__": annotations,
        }
        for field in self.storage_meta.fields:
            if not field.is_readable:
                continue
            type_ = self.wrap_type_for_strawberry(field.schema.python_type)
            annotations[field.name] = type_
        for link in self.storage_meta.links:
            if isinstance(link, BelongsTo):
                annotations[link.get_name()] = to_camel_case(link.storage_name)
                params[link.get_name()] = self.create_belongs_to_field(link)
            if isinstance(link, HasCount):
                annotations[link.get_name()] = int
                params[link.get_name()] = self.create_has_count_field(link)
            if isinstance(link, HasMany):
                annotations[link.get_name()] = to_camel_case(
                    f"{link.storage_name}_result_set"
                )
                params[link.get_name()] = self.create_has_many_field(link)

        type_name = _type_name(self.storage_meta.name)
        type_ = strawberry.type(type(type_name, (), params))
        self.types[self.storage_meta.name] = type_
        return type_

    def create_belongs_to_field(self, belongs_to: BelongsTo):
        belongs_to_type = belongs_to.storage_name
        if belongs_to.optional:
            belongs_to_type = Optional[belongs_to_type]

        async def resolver(root, info: Info) -> belongs_to_type:
            loader = self.get_data_loader(belongs_to.storage_name, info)
            key = str(getattr(root, belongs_to.id_field_name))
            return await loader.load(key)

        return _strawberry_field(belongs_to.name, resolver)

    def create_has_count_field(self, has_count: HasCount):
        def resolver(root, info: Info) -> int:
            authorization = self.get_authorization(info)
            key = self.storage_meta.key_config.to_key_str(root)
            search_filter = FieldFilter(has_count.id_field_name, FieldFilterOp.eq, key)
            storage = self.persisty_context.get_storage(
                has_count.storage_name, authorization
            )
            count = storage.count(search_filter)
            return count

        return _strawberry_field(has_count.name, resolver)

    def create_has_many_field(self, has_many: HasMany):
        result_set_type = f"{has_many.storage_name}_result_set"

        def resolver(root, info: Info) -> result_set_type:
            authorization = self.get_authorization(info)
            key = self.storage_meta.key_config.to_key_str(root)
            search_filter = FieldFilter(has_many.id_field_name, FieldFilterOp.eq, key)
            storage = self.persisty_context.get_storage(
                has_many.storage_name, authorization
            )
            result_set = storage.search(search_filter)
            item_marshaller = self.get_marshaller_for_type(self.get_item_type())
            result_set.results = [item_marshaller.load(r) for r in result_set.results]
            return result_set

        return _strawberry_field(has_many.name, resolver)

    def get_result_set_type(self):
        result_set_type = self.result_set_type
        if not result_set_type:
            # noinspection PyTypeChecker
            result_set_type = result_set_dataclass_for(self.get_item_type())
            result_set_type = strawberry.type(result_set_type)
            self.result_set_type = result_set_type
            self.types[to_snake_case(result_set_type.__name__)] = result_set_type
        return result_set_type

    def get_search_filter_factory_type(self) -> Type:
        search_filter_factory_type = self.search_filter_factory_type
        if not search_filter_factory_type:
            search_filter_factory_type = search_filter_dataclass_for(self.storage_meta)
            search_filter_factory_type = self.wrap_input_for_strawberry(
                search_filter_factory_type
            )
            self.search_filter_factory_type = search_filter_factory_type
        return search_filter_factory_type

    def get_search_order_factory_type(self):
        search_order_factory_type = self.search_order_factory_type
        if not search_order_factory_type:
            search_order_factory_type = search_order_dataclass_for(self.storage_meta)
            search_order_factory_type = self.wrap_input_for_strawberry(
                search_order_factory_type
            )
            self.search_order_factory_type = search_order_factory_type
        return search_order_factory_type

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
                if (
                    isinstance(field.write_transform, DefaultValueTransform)
                    and field.write_transform.default_value is None
                ):
                    params[field.name] = None

            if mode == WriteTransformMode.GENERATED:
                continue
            if mode == WriteTransformMode.OPTIONAL:
                type_ = Optional[type_]

            annotations[field.name] = type_

        params["__init__"] = _init
        params["__annotations__"] = annotations
        type_ = self.wrap_input_for_strawberry(
            dataclasses.dataclass(
                type(_type_name(f"create_{self.storage_meta.name}_input"), (), params)
            )
        )
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
        params = {"__init__": _init, "__annotations__": annotations}
        type_ = self.wrap_input_for_strawberry(
            dataclasses.dataclass(type(name, (), params))
        )
        self.update_input_type = type
        return type_

    def get_marshaller_for_type(self, type_: Type) -> MarshallerABC:
        marshaller = (
            self.persisty_context.schema_context.marshaller_context.get_marshaller(
                type_
            )
        )
        return marshaller

    def wrap_type_for_strawberry(self, type_: Type):
        """Wrap a nested dataclass structure in strawberry types"""
        origin = typing_inspect.get_origin(type_)
        if origin:
            origin = self.wrap_type_for_strawberry(origin)
            args = tuple(
                self.wrap_type_for_strawberry(a) for a in typing_inspect.get_args(type_)
            )
            return origin[args]
        if dataclasses.is_dataclass(type_):
            output = self.types.get(type_.__name__)
            if output:
                return output
            output = strawberry.type(type_, description=type_.__doc__)
            self.types[type_.__name__] = output
            for field in dataclasses.fields(output):
                field.type = self.wrap_type_for_strawberry(field.type)
            return output
        if inspect.isclass(type_) and issubclass(type_, Enum):
            strawberry_enum = self.enums.get(type_.__name__)
            if not strawberry_enum:
                # noinspection PyTypeChecker
                strawberry_enum = self.enums[type_.__name__] = strawberry.enum(type_)
            return strawberry_enum
        # Handle anything that strawberry has trouble with here...
        return type_

    def wrap_input_for_strawberry(self, type_: Type):
        """Wrap a nested dataclass structure in strawberry types"""
        origin = typing_inspect.get_origin(type_)
        if origin:
            origin = self.wrap_type_for_strawberry(origin)
            args = tuple(
                self.wrap_type_for_strawberry(a) for a in typing_inspect.get_args(type_)
            )
            return origin[args]
        if dataclasses.is_dataclass(type_):
            input = self.inputs.get(type_.__name__)
            if input:
                return input
            input = strawberry.input(type_, description=type_.__doc__)
            self.inputs[type_.__name__] = input
            for field in dataclasses.fields(input):
                field.type = self.wrap_input_for_strawberry(field.type)
            return input
        if inspect.isclass(type_) and issubclass(type_, Enum):
            strawberry_enum = self.enums.get(type_.__name__)
            if not strawberry_enum:
                # noinspection PyTypeChecker
                strawberry_enum = self.enums[type_.__name__] = strawberry.enum(type_)
            return strawberry_enum
        # Handle anything that strawberry has trouble with here...
        return type_

    def get_storage(self, authorization: Authorization):
        return self.persisty_context.get_storage(self.storage_meta.name, authorization)


def _strawberry_field(name: str, resolver: Callable) -> StrawberryField:
    field = strawberry.field(resolver=resolver)
    field.name = name
    return field


def _type_name(name: str) -> str:
    return "".join(p[:1].upper() + p[1:] for p in name.split("_"))


def _init(self, *_, **kwargs):
    for key in self.__annotations__:
        value = kwargs.get(key, UNDEFINED)
        setattr(self, key, value)


def _add_field(field: StrawberryField, fields: Dict[str, StrawberryField]):
    if field:
        fields[field.name] = field
