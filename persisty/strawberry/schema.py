from typing import Optional, Dict, Type, Set, List, ForwardRef

import strawberry
from dataclasses import is_dataclass, fields

import typing_inspect
from marshy.factory.optional_marshaller_factory import get_optional_type
from strawberry.annotation import StrawberryAnnotation
from strawberry.field import StrawberryField, UNRESOLVED
from strawberry.type import StrawberryContainer, StrawberryOptional
from strawberry.types.fields.resolver import StrawberryResolver

from persisty.access_control.authorization import Authorization, ROOT
from persisty.context import get_default_persisty_context, PersistyContext
from persisty.errors import PersistyError
from persisty.storage.storage_meta import StorageMeta
from persisty.strawberry.storage_schema_factory import StorageSchemaFactory
from persisty.util import to_snake_case


def new_schema_from_storage(
    authorization: Authorization = ROOT,
    persisty_context: Optional[PersistyContext] = None,
):
    if persisty_context is None:
        persisty_context = get_default_persisty_context()
    meta_storage = persisty_context.get_meta_storage(authorization)
    query_params: Dict = {}
    mutation_params: Dict = {}

    storage_meta_list = list(meta_storage.search_all())
    if not storage_meta_list:
        raise PersistyError("No storage detected in context!")

    marshaller = persisty_context.schema_context.marshaller_context.get_marshaller(
        StorageMeta
    )
    types = {}
    for storage_meta in storage_meta_list:
        storage_meta = marshaller.load(storage_meta)
        factory = StorageSchemaFactory(persisty_context, storage_meta, types)
        factory.add_to_schema(query_params, mutation_params)

    # the fields may have non globally resolvable forward references...
    resolved = set()
    for field in query_params.values():
        field.type = _resolve_type_futures(field.type, types, resolved)

    query_params["__annotations__"] = {f.name: f.type for f in query_params.values()}
    queries = strawberry.type(type("Query", (), query_params))

    mutation_params["__annotations__"] = {
        f.name: f.type for f in mutation_params.values()
    }
    mutations = strawberry.type(type("Mutation", (), mutation_params))

    schema = strawberry.Schema(queries, mutations)
    return schema


def _resolve_type_futures(type_, types: Dict[str, Type], resolved: Set):
    if isinstance(type_, str):
        type_ = types[to_snake_case(type_)]
    if isinstance(type_, StrawberryAnnotation):
        type_.type = _resolve_type_futures(type_.annotation, types, resolved)
        return type_
    if isinstance(type_, StrawberryContainer):
        type_.of_type = _resolve_type_futures(type_.of_type, types, resolved)
        return type_
    name = typing_inspect.get_forward_arg(type_)
    if name:
        type_ = types[to_snake_case(name)]
        return type_
    optional_type = get_optional_type(type_)
    if optional_type:
        return StrawberryOptional(_resolve_type_futures(optional_type, types, resolved))
    origin = typing_inspect.get_origin(type_)
    if origin:
        args = tuple(_resolve_type_futures(a, types, resolved) for a in typing_inspect.get_args(type_))
        if origin is list:
            return List[args]
        else:
            return origin[args]
    if is_dataclass(type_):
        if type_.__name__ in resolved:
            return type_
        resolved.add(type_.__name__)
        # noinspection PyDataclass
        for field in fields(type_):
            if isinstance(field, StrawberryField):
                if field.type is UNRESOLVED:
                    resolver = field.base_resolver
                    field_type = resolver.signature.return_annotation
                    field_type = _resolve_type_futures(field_type, types, resolved)
                    resolver_override = StrawberryResolver(resolver.wrapped_func, type_override=field_type)
                    field.base_resolver = resolver_override
            else:
                field.type = _resolve_type_futures(field.type, types, resolved)
    return type_
