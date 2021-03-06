"""
Context for entities. Differs from persistence_context in that it is global and typically configured before
application start.
"""
from typing import TypeVar, Type, Union, get_type_hints, Optional

from dataclasses import dataclass

from persisty.entity.entity import Entity
from persisty.entity.entity_property_descriptor import EntityPropertyDescriptor
from persisty.errors import PersistyError
from persisty.obj_storage.stored import get_storage_meta
from persisty.storage.storage_meta import StorageMeta
from persisty.util import to_camel_case, UNDEFINED

T = TypeVar("T")
PersistyContext = "persisty.context.PersistyContext"
_entity_types = {}


def get_named_entity_type(name: str) -> Type[Union[T, Entity]]:
    entity_type = _entity_types.get(name)
    return entity_type


def create_entity_type(
    type_: T, persisty_context: Optional[PersistyContext] = None
) -> Type[Union[T, Entity]]:
    entity_type = _entity_types.get(type_.__name__)
    if entity_type:
        raise PersistyError(f"entity_already_exists:{type_}")
    if not persisty_context:
        from persisty.context import get_default_persisty_context

        persisty_context = get_default_persisty_context()
    storage_meta = get_storage_meta(type_)
    dumped = persisty_context.schema_context.marshaller_context.dump(
        storage_meta, StorageMeta
    )
    existing_storage_meta = persisty_context.meta_storage.read(storage_meta.name)
    if not existing_storage_meta:
        persisty_context.meta_storage.create(dumped)
    elif existing_storage_meta != dumped:
        raise PersistyError(
            f"Storage meta did not match existing: {storage_meta.name} (Maybe a migration is required?)"
        )
    annotations = get_type_hints(type_)
    entity_properties = [EntityPropertyDescriptor(key) for key in annotations]
    params = {p.name: p for p in entity_properties}
    for relation in storage_meta.relations:
        params[relation.get_name()] = relation.to_property_descriptor()
    params["__annotations__"] = get_type_hints(type_)
    params["__persisty_storage_meta__"] = storage_meta
    params["__persisty_context__"] = persisty_context
    params["__persisty_dataclass_type__"] = dataclass_type(storage_meta)
    marshaller_context = persisty_context.schema_context.marshaller_context
    params["__marshaller__"] = marshaller_context.get_marshaller(params["__persisty_dataclass_type__"])
    entity_type = type(type_.__name__, (Entity,), params)
    _entity_types[type_.__name__] = entity_type
    return entity_type


def dataclass_type(storage_meta: StorageMeta):
    annotations = {f.name: f.schema.python_type for f in storage_meta.fields}
    params = {a: UNDEFINED for a in annotations}
    params['__annotations__'] = annotations
    type_ = dataclass(type(to_camel_case(storage_meta.name), (), params))
    return type_
