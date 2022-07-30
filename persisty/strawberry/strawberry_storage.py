import dataclasses
from enum import Enum
from typing import Dict, Type, List, Optional
import typing_inspect

# noinspection PyPackageRequirements
import strawberry
from marshy.marshaller_context import MarshallerContext
# noinspection PyPackageRequirements
from strawberry.field import StrawberryField
# noinspection PyPackageRequirements
from strawberry.types import Info

from persisty.access_control.authorization import Authorization, ROOT
from persisty.context import PersistyContext
from persisty.field.field_filter import FieldFilter, FieldFilterOp
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC

from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_field import SearchOrderField
from persisty.storage.storage_meta import StorageMeta


def add_storage_to_schema(
    storage_meta: StorageMeta,
    persisty_context: PersistyContext,
    query: Dict[str, StrawberryField],
    mutation: Dict[str, StrawberryField],
):
    strawberry_item_type = create_strawberry_item_type_for_storage(storage_meta)
    add_field(
        create_read_field(
            storage_meta, strawberry_item_type, persisty_context
        ),
        query,
    )
    add_field(
        create_read_all_field(
            storage_meta, strawberry_item_type, persisty_context
        ),
        query,
    )
    add_field(
        create_search_field(
            storage_meta, strawberry_item_type, persisty_context
        ),
        query
    )


def create_strawberry_item_type_for_storage(storage_meta: StorageMeta):
    annotations = {}
    for field in storage_meta.fields:
        type_ = create_strawberry_type(field.schema.python_type)
        if field.is_nullable:
            type_ = Optional[type_]
        annotations[field.name] = type_
    type_ = strawberry.type(
        type(storage_meta.name, (), dict(__annotations__=annotations))
    )
    return type_


def create_strawberry_type(type_: Type):
    origin = typing_inspect.get_origin(type_)
    if origin:
        origin = create_strawberry_type(type_)
        args = tuple(create_strawberry_type(a) for a in typing_inspect.get_args(type_))
        return origin[args]
    if dataclasses.is_dataclass(type_):
        return strawberry.type(type_)
    # TODO: Handle anything that strawberry has trouble with here...
    return type_


def create_strawberry_result_set_type(type_: Type):
    name = f"{type_.__name__}_result_set"
    params = {
        "__annotations__": {"results": List[type_], "next_page_key": Optional[str]}
    }
    type_ = strawberry.type(type(name, (), params))
    return type_


def add_field(field: StrawberryField, fields: Dict[str, StrawberryField]):
    fields[field.name] = field


def create_read_field(
    storage_meta: StorageMeta,
    strawberry_item_type: Type,
    persisty_context: PersistyContext,
) -> StrawberryField:
    def resolver(key: str, info: Info) -> strawberry_item_type:
        authorization = get_authorization(info)
        storage = persisty_context.get_storage(storage_meta.name, authorization)
        read = storage.read(key)
        marshaller_context = persisty_context.schema_context.marshaller_context
        loaded = marshaller_context.load(strawberry_item_type, read)
        return loaded

    field = strawberry.field(resolver=resolver)
    field.name = f"read_{storage_meta.name}"
    field.type = strawberry_item_type
    return field


def create_read_all_field(
    storage_meta: StorageMeta,
    strawberry_item_type: Type,
    persisty_context: PersistyContext,
) -> StrawberryField:
    async def resolver(keys: List[str], info: Info) -> List[strawberry_item_type]:
        authorization = get_authorization(info)
        storage = persisty_context.get_storage(storage_meta.name, authorization)
        read = storage.read_all(keys)
        marshaller = persisty_context.schema_context.marshaller_context.get_marshaller(strawberry_item_type)
        loaded = [marshaller.load(r) for r in read]
        return loaded

    field = strawberry.field(resolver=resolver)
    field.name = f"read_all_{storage_meta.name}"
    field.type = List[strawberry_item_type]
    return field


def create_search_field(
    storage_meta: StorageMeta,
    strawberry_item_type: Type,
    persisty_context: PersistyContext,
) -> StrawberryField:
    strawberry_result_set_type = create_strawberry_result_set_type(strawberry_item_type)
    strawberry_search_filter_factory_type = create_strawberry_search_filter_factory_type(storage_meta)
    strawberry_search_order_factory_type = create_strawberry_search_order_factory_type(storage_meta)

    def resolver(
            info: Info,
            search_filter: Optional[strawberry_search_filter_factory_type] = None,
            search_order: Optional[strawberry_search_order_factory_type] = None,
            page_key: Optional[str] = None,
            limit: int = storage_meta.batch_size
    ) -> strawberry_result_set_type:
        authorization = get_authorization(info)
        storage = persisty_context.get_storage(storage_meta.name, authorization)
        marshaller_context = persisty_context.schema_context.marshaller_context
        search_filter = create_search_filter(search_filter, storage_meta, marshaller_context)
        search_order = create_search_order(search_order)
        result_set = storage.search(search_filter, search_order, page_key, limit)
        result_set.results = [marshaller_context.load(strawberry_item_type, r) for r in result_set.results]
        return result_set

    field = strawberry.field(resolver=resolver)
    field.name = f"search_{storage_meta.name}"
    field.type = strawberry_result_set_type
    return field


def create_strawberry_search_filter_factory_type(storage_meta: StorageMeta):
    annotations = {}
    for field in storage_meta.fields:
        for op in field.permitted_filter_ops:
            filter_name = f"{field.name}_{op.name}"
            field_type = field.schema.python_type
            if op in (FieldFilterOp.exists, FieldFilterOp.not_exists):
                field_type = bool
            annotations[filter_name] = Optional[field_type]
    params = {a: None for a in annotations}
    params['__annotations__'] = annotations
    type_ = strawberry.input(type(f"{storage_meta.name}_search_filter", (), params))
    return type_


def create_strawberry_search_order_factory_type(storage_meta: StorageMeta):
    fields = {f.name: f.name for f in storage_meta.fields if f.is_sortable}
    if not fields:
        return
    params = {
        'desc': False,
        '__annotations__': {
            'field': strawberry.enum(Enum(f"{storage_meta.name}_search_field", fields)),
            'desc': Optional[bool]
        }
    }
    type_ = strawberry.input(type(f"{storage_meta.name}_search_order", (), params))
    return type_


def get_authorization(info: Info) -> Authorization:
    try:
        return info.context["authorization"]
    except KeyError:
        return ROOT


def create_search_filter(obj, storage_meta: StorageMeta, marshaller_context: MarshallerContext) -> SearchFilterABC:
    search_filter = INCLUDE_ALL
    for field in storage_meta.fields:
        for op in field.permitted_filter_ops:
            filter_name = f"{field.name}_{op.name}"
            if hasattr(obj, filter_name):
                value = getattr(obj, filter_name)
                if value is not None:
                    # In graphql, null and undefined are the same thing. This means we effectively can't search for
                    # null values like this
                    # noinspection PyTypeChecker
                    value = marshaller_context.load(field.schema.python_type, value)
                    field_filter = FieldFilter(field.name, op, value)
                    search_filter &= field_filter
    return search_filter


def create_search_order(obj) -> Optional[SearchOrder]:
    if not obj:
        return
    field = getattr(obj, "field")
    desc = getattr(obj, "desc")
    return SearchOrder((SearchOrderField(field.value, desc),))
